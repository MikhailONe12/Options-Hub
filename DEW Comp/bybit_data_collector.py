import requests
import sqlite3
import yaml
import os
import datetime
import logging
import time
import signal
import sys
from typing import Dict, List, Optional, Tuple, Any
import json
from logging.handlers import RotatingFileHandler

# Настройка логирования - отключить вывод в терминал и писать в файл вместо этого
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            'bybit_collector.log',
            maxBytes=1_048_576,  # 1 MB (уменьшено для контроля размера)
            backupCount=1,       # Сохранять 1 бэкап файл
            encoding='utf-8'
        )
        # Закомментируйте или удалите StreamHandler, чтобы предотвратить вывод в терминал
        # logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Максимальный размер данных для логирования (в символах)
MAX_LOG_DATA_SIZE = 500

class BybitDataCollector:
    def __init__(self, config_path: str = "Config_API_Bybit.yaml"):
        """
        Инициализация сборщика данных Bybit с файлом конфигурации
        """
        # Determine the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # If config_path is a relative path, make it relative to the script directory
        if not os.path.isabs(config_path):
            config_path = os.path.join(script_dir, config_path)
        
        logger.debug(f"Initializing BybitDataCollector with config path: {config_path}")
        self.config_path = config_path
        self.load_config()
        self.ensure_data_directories()
        logger.debug("BybitDataCollector initialized successfully")
        
    def load_config(self):
        """
        Загрузка конфигурации из YAML файла
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self.config = yaml.safe_load(file)
            logger.info("Configuration loaded successfully")
            
            # Логирование деталей конфигурации для отладки
            logger.debug(f"Configuration details:")
            logger.debug(f"  Data folder: {self.config.get('data_folder', 'N/A')}")
            logger.debug(f"  Tickers: {self.config.get('tickers', [])}")
            logger.debug(f"  Trading days: {self.config.get('trading_days', [])}")
            logger.debug(f"  Futures quote coins: {self.config.get('futures', {}).get('quote_coins', [])}")
            
            # Проверить, включен ли режим отладки
            debug_enabled = self.config.get('collection', {}).get('debug', False)
            logger.debug(f"Debug setting in config: {debug_enabled}")
            
            if debug_enabled:
                logging.getLogger().setLevel(logging.DEBUG)
                logger.info("Debug mode enabled")
            else:
                logger.info("Debug mode disabled")
                
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            logger.exception(e)  # Добавим полную трассировку исключения
            raise
            
    def ensure_data_directories(self):
        """
        Убедиться, что существуют директории данных для каждого тикера
        """
        data_folder = self.config.get('data_folder', './bybit_data')
        tickers = self.config.get('tickers', [])
        
        logger.debug(f"Ensuring data directories: data_folder={data_folder}, tickers={tickers}")
        
        # Создать основную папку данных, если она не существует
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
            logger.info(f"Created main data folder: {data_folder}")
            
        # Создать папку для каждого тикера
        for ticker in tickers:
            ticker_folder = os.path.join(data_folder, f"Bybit_{ticker}")
            if not os.path.exists(ticker_folder):
                os.makedirs(ticker_folder)
                logger.info(f"Created folder for ticker {ticker}: {ticker_folder}")
            else:
                logger.debug(f"Folder for ticker {ticker} already exists: {ticker_folder}")
                
    def is_bybit_trading_day(self) -> bool:
        """
        Проверить, является ли сегодня торговым днем согласно календарю Bybit
        Bybit работает 24/7, но мы можем использовать настройки из конфигурации
        """
        try:
            # Получить сегодняшнюю дату
            today = datetime.datetime.now().date()
            
            # По умолчанию True, так как Bybit работает 24/7
            trading_days = self.config.get('trading_days', ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            today_name = datetime.datetime.now().strftime('%A')
            is_trading_day = today_name in trading_days
            
            logger.debug(f"Trading day check: today={today_name}, allowed_days={trading_days}, is_trading_day={is_trading_day}")
            return is_trading_day
        except Exception as e:
            # По умолчанию True, чтобы избежать пропуска данных при проблемах с подключением
            logger.error(f"Error checking trading day: {e}")
            return True
            
    def is_market_open(self) -> bool:
        """
        Проверить, открыта ли биржа в текущее время
        Bybit работает 24/7, но мы можем использовать настройки из конфигурации
        """
        try:
            # Получить текущее время
            current_time = datetime.datetime.now()
            
            # Проверить, является ли сегодня торговым днем
            if not self.is_bybit_trading_day():
                return False
                
            # Получить время открытия и закрытия рынка из конфигурации
            collection_config = self.config.get('collection', {})
            open_time_str = collection_config.get('market_open_time', '00:00')
            close_time_str = collection_config.get('market_close_time', '23:59')
            
            # Преобразовать строки времени в объекты time
            market_open = datetime.datetime.strptime(open_time_str, '%H:%M').time()
            market_close = datetime.datetime.strptime(close_time_str, '%H:%M').time()
            
            current_time_only = current_time.time()
            
            # Проверить, находится ли текущее время в рамках торговой сессии
            is_open = market_open <= current_time_only <= market_close
            logger.debug(f"Market hours check: open={market_open}, close={market_close}, current={current_time_only}, is_open={is_open}")
            return is_open
        except Exception as e:
            logger.error(f"Error checking market hours: {e}")
            # По умолчанию True, чтобы избежать пропуска данных при проблемах с подключением
            return True

    def get_bybit_api_data(self, endpoint: str, params: Optional[dict] = None) -> Optional[Dict]:
        """
        Сделать запрос к API Bybit и вернуть разобранный JSON ответ
        """
        base_url = self.config['bybit_api']['base_url']
        url = f"{base_url}{endpoint}"
        
        logger.debug(f"Making API request to {url} with params: {params}")
        
        try:
            headers = {'User-Agent': 'Bybit Data Collector 1.0'}
            
            # Получить таймаут из конфигурации или использовать значение по умолчанию
            timeout = 15  # Сниженный дефолтный таймаут, чтобы не зависать по 60с на запрос
            if hasattr(self, 'config') and 'settings' in self.config:
                timeout = self.config['settings'].get('api_timeout', 60)
            
            # Попробовать выполнить запрос несколько раз в случае ошибок
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = requests.get(url, timeout=timeout, headers=headers, params=params)
                    response.raise_for_status()
                    
                    # Разобрать JSON ответ
                    data = response.json()
                    # Не логировать полный ответ API - слишком большой объем данных
                    result_count = len(data.get('result', {}).get('list', [])) if isinstance(data.get('result'), dict) else 0
                    logger.debug(f"API response from {url}: retCode={data.get('retCode')}, items={result_count}")
                    return data
                except requests.exceptions.Timeout:
                    if attempt < max_retries - 1:
                        logger.warning(f"Timeout occurred for {url}, retrying... (attempt {attempt + 1})")
                        time.sleep(2 ** attempt)  # Экспоненциальная задержка
                        continue
                    else:
                        logger.error(f"Timeout occurred for {url} after {max_retries} attempts")
                        raise
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Request error for {url}: {e}, retrying... (attempt {attempt + 1})")
                        time.sleep(2 ** attempt)  # Экспоненциальная задержка
                        continue
                    else:
                        logger.error(f"Request error for {url}: {e} after {max_retries} attempts")
                        raise
                        
        except Exception as e:
            logger.error(f"Error fetching data from {url}: {e}")
            logger.exception(e)  # Добавим полную трассировку исключения
            return None
            
    def get_options_data(self, base_coin: str) -> Optional[List[Dict]]:
        """
        Получить данные опционов для конкретной монеты
        """
        try:
            # Получить все опционные инструменты для базовой монеты
            endpoint = self.config['bybit_api']['endpoints']['instruments']
            params = {
                'category': 'option',
                'baseCoin': base_coin,
                'limit': '1000'  # Увеличен лимит для получения всех опционов
            }
            
            data = self.get_bybit_api_data(endpoint, params)
            
            if data is None or data.get('retCode') != 0:
                logger.error(f"Failed to get options data for {base_coin}")
                return None
                
            # Извлечь список опционов
            options_list = data.get('result', {}).get('list', [])
            logger.info(f"Retrieved {len(options_list)} options for {base_coin}")
            # Не логировать полный список - слишком большой объем данных
            
            if not options_list:
                logger.warning(f"No options found for {base_coin}")
                return None
                
            # Форматировать данные в нужный формат
            formatted_options = []
            for option in options_list:
                formatted_option = {
                    'symbol': option.get('symbol'),
                    'optionsType': option.get('optionsType'),  # Call or Put
                    'status': option.get('status'),
                    'baseCoin': option.get('baseCoin'),
                    'quoteCoin': option.get('quoteCoin'),
                    'settleCoin': option.get('settleCoin'),
                    'launchTime': option.get('launchTime'),
                    'deliveryTime': option.get('deliveryTime'),
                    'deliveryFeeRate': option.get('deliveryFeeRate'),
                    # Price filter
                    'minPrice': option.get('priceFilter', {}).get('minPrice'),
                    'maxPrice': option.get('priceFilter', {}).get('maxPrice'),
                    'tickSize': option.get('priceFilter', {}).get('tickSize'),
                    # Lot size filter
                    'maxOrderQty': option.get('lotSizeFilter', {}).get('maxOrderQty'),
                    'minOrderQty': option.get('lotSizeFilter', {}).get('minOrderQty'),
                    'qtyStep': option.get('lotSizeFilter', {}).get('qtyStep'),
                    'displayName': option.get('displayName')
                }
                formatted_options.append(formatted_option)
                
            return formatted_options
        except Exception as e:
            logger.error(f"Error getting options data for {base_coin}: {e}")
            logger.exception(e)  # Добавим полную трассировку исключения
            return None
        
    def get_options_tickers(self, base_coin: str) -> Optional[List[Dict]]:
        """
        Получить тикерные данные для опционов базовой монеты
        """
        try:
            endpoint = self.config['bybit_api']['endpoints']['tickers']
            params = {
                'category': 'option',
                'baseCoin': base_coin,
                'limit': '1000'  # Увеличен лимит для получения всех тикеров
            }
            
            data = self.get_bybit_api_data(endpoint, params)
            
            if data is None or data.get('retCode') != 0:
                logger.error(f"Failed to get options tickers for {base_coin}")
                return None
                
            # Извлечь список тикеров
            tickers_list = data.get('result', {}).get('list', [])
            logger.info(f"Retrieved {len(tickers_list)} options tickers for {base_coin}")
            # Не логировать полный список тикеров - слишком большой объем данных
            
            if not tickers_list:
                logger.warning(f"No options tickers found for {base_coin}")
                return None
                
            # Форматировать данные в нужный формат
            formatted_tickers = []
            for ticker in tickers_list:
                formatted_ticker = {
                    'symbol': ticker.get('symbol'),
                    'bid1Price': ticker.get('bid1Price'),
                    'bid1Size': ticker.get('bid1Size'),
                    'bid1Iv': ticker.get('bid1Iv'),
                    'ask1Price': ticker.get('ask1Price'),
                    'ask1Size': ticker.get('ask1Size'),
                    'ask1Iv': ticker.get('ask1Iv'),
                    'lastPrice': ticker.get('lastPrice'),
                    'highPrice24h': ticker.get('highPrice24h'),
                    'lowPrice24h': ticker.get('lowPrice24h'),
                    'markPrice': ticker.get('markPrice'),
                    'indexPrice': ticker.get('indexPrice'),
                    'markIv': ticker.get('markIv'),
                    'underlyingPrice': ticker.get('underlyingPrice'),
                    'openInterest': ticker.get('openInterest'),
                    'turnover24h': ticker.get('turnover24h'),
                    'volume24h': ticker.get('volume24h'),
                    'totalVolume': ticker.get('totalVolume'),
                    'totalTurnover': ticker.get('totalTurnover'),
                    'delta': ticker.get('delta'),
                    'gamma': ticker.get('gamma'),
                    'vega': ticker.get('vega'),
                    'theta': ticker.get('theta'),
                    'predictedDeliveryPrice': ticker.get('predictedDeliveryPrice'),
                    'change24h': ticker.get('change24h')
                }
                formatted_tickers.append(formatted_ticker)
                
            return formatted_tickers
        except Exception as e:
            logger.error(f"Error getting options tickers for {base_coin}: {e}")
            logger.exception(e)  # Добавим полную трассировку исключения
            return None
            
    def get_futures_instruments(self, base_coin: str) -> Optional[List[Dict]]:
        """
        Получить данные фьючерсных инструментов для конкретной монеты
        """
        try:
            # Получить все фьючерсные инструменты для базовой монеты
            endpoint = self.config['bybit_api']['endpoints']['instruments']
            params = {
                'category': 'linear',
                'baseCoin': base_coin,
                'limit': '1000'  # Увеличен лимит для получения всех фьючерсов
            }
            
            data = self.get_bybit_api_data(endpoint, params)
            
            if data is None or data.get('retCode') != 0:
                logger.error(f"Failed to get futures instruments for {base_coin}")
                return None
                
            # Извлечь список фьючерсов
            futures_list = data.get('result', {}).get('list', [])
            logger.info(f"Retrieved {len(futures_list)} futures instruments for {base_coin}")
            # Не логировать полный список фьючерсов - слишком большой объем данных
            
            if not futures_list:
                logger.warning(f"No futures found for {base_coin}")
                return None
                
            # Получить список разрешенных тикеров из конфигурации
            allowed_tickers = self.config.get('tickers', [])
            
            # Получить список разрешенных quote coins из конфигурации
            allowed_quote_coins = self.config.get('futures', {}).get('quote_coins', ['USDT'])
            
            logger.info(f"Allowed tickers: {allowed_tickers}")
            logger.info(f"Allowed quote coins: {allowed_quote_coins}")
            
            # Фильтровать фьючерсы, оставляя только те, которые соответствуют:
            # 1. Базовая монета в списке разрешенных тикеров
            # 2. Quote монета в списке разрешенных quote coins
            # 3. Точное совпадение формата: {base_coin}{quote_coin} или {base_coin}{quote_coin}-[дата]
            filtered_futures = []
            for future in futures_list:
                symbol = future.get('symbol', '')
                quote_coin = future.get('quoteCoin', '')
                base_coin_from_future = future.get('baseCoin', '')
                
                # Если baseCoin или quoteCoin отсутствуют, попробуем извлечь из символа
                # Улучшенная логика: проверяем точное совпадение формата
                if not base_coin_from_future or not quote_coin:
                    # Извлечь base_coin и quote_coin из символа
                    for allowed_ticker in allowed_tickers:
                        if symbol.startswith(allowed_ticker):
                            # Проверяем каждый разрешенный quote coin
                            for allowed_quote in allowed_quote_coins:
                                # Точный формат: {base_coin}{quote_coin}
                                exact_match = f"{allowed_ticker}{allowed_quote}"
                                if symbol == exact_match:
                                    base_coin_from_future = allowed_ticker
                                    quote_coin = allowed_quote
                                    break
                                
                                # Формат с датой: {base_coin}{quote_coin}-[дата]
                                date_prefix = f"{allowed_ticker}{allowed_quote}-"
                                if symbol.startswith(date_prefix):
                                    base_coin_from_future = allowed_ticker
                                    quote_coin = allowed_quote
                                    break
                            
                            if base_coin_from_future:
                                break
                
                logger.debug(f"Checking future: {symbol}")
                logger.debug(f"  baseCoin from future: '{base_coin_from_future}'")
                logger.debug(f"  quoteCoin from future: '{quote_coin}'")
                logger.debug(f"  Allowed tickers: {allowed_tickers}")
                logger.debug(f"  Allowed quote coins: {allowed_quote_coins}")
                
                # Проверить, есть ли базовая монета в списке разрешенных тикеров
                # Проверяем, что базовая монета СОВПАДАЕТ с запрашиваемой base_coin
                base_coin_match = base_coin_from_future == base_coin
                # Проверить, есть ли quote монета в списке разрешенных quote coins
                quote_coin_match = quote_coin in allowed_quote_coins
                
                logger.debug(f"  Base coin match: {base_coin_match}")
                logger.debug(f"  Quote coin match: {quote_coin_match}")
                
                if base_coin_match and quote_coin_match:
                    # Добавим извлеченные значения, если они отсутствовали
                    future_with_coins = future.copy()
                    if not future_with_coins.get('baseCoin'):
                        future_with_coins['baseCoin'] = base_coin_from_future
                    if not future_with_coins.get('quoteCoin'):
                        future_with_coins['quoteCoin'] = quote_coin
                    filtered_futures.append(future_with_coins)
                    logger.debug(f"Added {symbol} to filtered futures")
                else:
                    logger.debug(f"Skipped {symbol} - base_coin_match: {base_coin_match}, quote_coin_match: {quote_coin_match}")
            
            logger.info(f"Filtered to {len(filtered_futures)} futures instruments for {base_coin}")
            
            if not filtered_futures:
                logger.warning(f"No futures instruments found for {base_coin} with quote coins {allowed_quote_coins}")
                # Log some examples for debugging
                if futures_list:
                    example_future = futures_list[0]
                    logger.info(f"Example future: symbol={example_future.get('symbol')}, baseCoin={example_future.get('baseCoin')}, quoteCoin={example_future.get('quoteCoin')}")
                return None
                
            # Форматировать данные в нужный формат
            formatted_futures = []
            for future in filtered_futures:
                formatted_future = {
                    'symbol': future.get('symbol'),
                    'contractType': future.get('contractType'),
                    'status': future.get('status'),
                    'baseCoin': future.get('baseCoin'),
                    'quoteCoin': future.get('quoteCoin'),
                    'settleCoin': future.get('settleCoin'),
                    'symbolType': future.get('symbolType'),
                    'launchTime': future.get('launchTime'),
                    'deliveryTime': future.get('deliveryTime'),
                    'deliveryFeeRate': future.get('deliveryFeeRate'),
                    'priceScale': future.get('priceScale'),
                    # Leverage filter
                    'minLeverage': future.get('leverageFilter', {}).get('minLeverage'),
                    'maxLeverage': future.get('leverageFilter', {}).get('maxLeverage'),
                    'leverageStep': future.get('leverageFilter', {}).get('leverageStep'),
                    # Price filter
                    'minPrice': future.get('priceFilter', {}).get('minPrice'),
                    'maxPrice': future.get('priceFilter', {}).get('maxPrice'),
                    'tickSize': future.get('priceFilter', {}).get('tickSize'),
                    # Lot size filter
                    'minNotionalValue': future.get('lotSizeFilter', {}).get('minNotionalValue'),
                    'maxOrderQty': future.get('lotSizeFilter', {}).get('maxOrderQty'),
                    'maxMktOrderQty': future.get('lotSizeFilter', {}).get('maxMktOrderQty'),
                    'minOrderQty': future.get('lotSizeFilter', {}).get('minOrderQty'),
                    'qtyStep': future.get('lotSizeFilter', {}).get('qtyStep'),
                    'unifiedMarginTrade': future.get('unifiedMarginTrade'),
                    'fundingInterval': future.get('fundingInterval'),
                    'upperFundingRate': future.get('upperFundingRate'),
                    'lowerFundingRate': future.get('lowerFundingRate'),
                    'displayName': future.get('displayName'),
                    'copyTrading': future.get('copyTrading'),
                    'forbidUplWithdrawal': future.get('forbidUplWithdrawal')
                }
                formatted_futures.append(formatted_future)
                
            logger.info(f"Formatted {len(formatted_futures)} futures instruments for {base_coin}")
            # Не логировать первый элемент - экономия места в логе
            return formatted_futures
        except Exception as e:
            logger.error(f"Error getting futures instruments for {base_coin}: {e}")
            logger.exception(e)  # Добавим полную трассировку исключения
            return None
            
    def get_futures_tickers(self, base_coin: str) -> Optional[List[Dict]]:
        """
        Получить тикерные данные для фьючерсов базовой монеты
        """
        try:
            endpoint = self.config['bybit_api']['endpoints']['tickers']
            params = {
                'category': 'linear',
                'baseCoin': base_coin,
                'limit': '1000'  # Увеличен лимит для получения всех тикеров
            }
            
            data = self.get_bybit_api_data(endpoint, params)
            
            if data is None or data.get('retCode') != 0:
                logger.error(f"Failed to get futures tickers for {base_coin}")
                return None
                
            # Извлечь список тикеров
            tickers_list = data.get('result', {}).get('list', [])
            logger.info(f"Retrieved {len(tickers_list)} futures tickers for {base_coin}")
            # Не логировать полный список тикеров - слишком большой объем данных
            
            if not tickers_list:
                logger.warning(f"No futures tickers found for {base_coin}")
                return None
                
            # Получить список разрешенных тикеров из конфигурации
            allowed_tickers = self.config.get('tickers', [])
            
            # Получить список разрешенных quote coins из конфигурации
            allowed_quote_coins = self.config.get('futures', {}).get('quote_coins', ['USDT'])
            
            logger.info(f"Allowed tickers: {allowed_tickers}")
            logger.info(f"Allowed quote coins: {allowed_quote_coins}")
            
            # Фильтровать тикеры, оставляя только те, которые соответствуют:
            # 1. Базовая монета в списке разрешенных тикеров
            # 2. Quote монета в списке разрешенных quote coins
            # 3. Точное совпадение формата: {base_coin}{quote_coin} или {base_coin}{quote_coin}-[дата]
            filtered_tickers = []
            for ticker in tickers_list:
                symbol = ticker.get('symbol', '')
                
                # Извлечь base_coin и quote_coin из символа, так как они отсутствуют в данных API
                # Формат символа: BTCUSDT, BTCUSDT-26DEC25 и т.д.
                base_coin_from_symbol = None
                quote_coin_from_symbol = None
                
                # Улучшенная логика: проверяем точное совпадение формата
                for allowed_ticker in allowed_tickers:
                    if symbol.startswith(allowed_ticker):
                        # Проверяем каждый разрешенный quote coin
                        for allowed_quote in allowed_quote_coins:
                            # Точный формат: {base_coin}{quote_coin}
                            exact_match = f"{allowed_ticker}{allowed_quote}"
                            if symbol == exact_match:
                                base_coin_from_symbol = allowed_ticker
                                quote_coin_from_symbol = allowed_quote
                                break
                            
                            # Формат с датой: {base_coin}{quote_coin}-[дата]
                            date_prefix = f"{allowed_ticker}{allowed_quote}-"
                            if symbol.startswith(date_prefix):
                                base_coin_from_symbol = allowed_ticker
                                quote_coin_from_symbol = allowed_quote
                                break
                        
                        if base_coin_from_symbol:
                            break
                
                logger.debug(f"Checking ticker: {symbol}")
                logger.debug(f"  baseCoin from symbol: '{base_coin_from_symbol}'")
                logger.debug(f"  quoteCoin from symbol: '{quote_coin_from_symbol}'")
                logger.debug(f"  Allowed tickers: {allowed_tickers}")
                logger.debug(f"  Allowed quote coins: {allowed_quote_coins}")
                
                # Проверить, есть ли базовая монета в списке разрешенных тикеров
                # Проверяем, что базовая монета СОВПАДАЕТ с запрашиваемой base_coin
                base_coin_match = base_coin_from_symbol == base_coin
                # Проверить, есть ли quote монета в списке разрешенных quote coins
                quote_coin_match = quote_coin_from_symbol in allowed_quote_coins
                
                logger.debug(f"  Base coin match: {base_coin_match}")
                logger.debug(f"  Quote coin match: {quote_coin_match}")
                
                if base_coin_match and quote_coin_match:
                    # Добавим извлеченные значения в тикер для последующего использования
                    ticker_with_coins = ticker.copy()
                    ticker_with_coins['baseCoin'] = base_coin_from_symbol
                    ticker_with_coins['quoteCoin'] = quote_coin_from_symbol
                    filtered_tickers.append(ticker_with_coins)
                    logger.debug(f"Added {symbol} to filtered tickers")
                else:
                    logger.debug(f"Skipped {symbol} - base_coin_match: {base_coin_match}, quote_coin_match: {quote_coin_match}")
            
            logger.info(f"Filtered to {len(filtered_tickers)} futures tickers for {base_coin}")
            
            if not filtered_tickers:
                logger.warning(f"No futures tickers found for {base_coin} with quote coins {allowed_quote_coins}")
                # Log some examples for debugging
                if tickers_list:
                    example_ticker = tickers_list[0]
                    logger.info(f"Example ticker: symbol={example_ticker.get('symbol')}")
                return None
                
            # Форматировать данные в нужный формат
            formatted_tickers = []
            for ticker in filtered_tickers:
                formatted_ticker = {
                    'symbol': ticker.get('symbol'),
                    'lastPrice': ticker.get('lastPrice'),
                    'indexPrice': ticker.get('indexPrice'),
                    'markPrice': ticker.get('markPrice'),
                    'prevPrice24h': ticker.get('prevPrice24h'),
                    'price24hPcnt': ticker.get('price24hPcnt'),
                    'highPrice24h': ticker.get('highPrice24h'),
                    'lowPrice24h': ticker.get('lowPrice24h'),
                    'prevPrice1h': ticker.get('prevPrice1h'),
                    'openInterest': ticker.get('openInterest'),
                    'openInterestValue': ticker.get('openInterestValue'),
                    'turnover24h': ticker.get('turnover24h'),
                    'volume24h': ticker.get('volume24h'),
                    'fundingRate': ticker.get('fundingRate'),
                    'nextFundingTime': ticker.get('nextFundingTime'),
                    'predictedDeliveryPrice': ticker.get('predictedDeliveryPrice'),
                    'basisRate': ticker.get('basisRate'),
                    'basis': ticker.get('basis'),
                    'deliveryFeeRate': ticker.get('deliveryFeeRate'),
                    'deliveryTime': ticker.get('deliveryTime'),
                    'bid1Price': ticker.get('bid1Price'),
                    'bid1Size': ticker.get('bid1Size'),
                    'ask1Price': ticker.get('ask1Price'),
                    'ask1Size': ticker.get('ask1Size'),
                    'preOpenPrice': ticker.get('preOpenPrice'),
                    'preQty': ticker.get('preQty'),
                    'curPreListingPhase': ticker.get('curPreListingPhase'),
                    'fundingIntervalHour': ticker.get('fundingIntervalHour'),
                    'fundingCap': ticker.get('fundingCap'),
                    'basisRateYear': ticker.get('basisRateYear'),
                    'baseCoin': ticker.get('baseCoin'),  # Из нашего добавления
                    'quoteCoin': ticker.get('quoteCoin')  # Из нашего добавления
                }
                formatted_tickers.append(formatted_ticker)
                
            logger.info(f"Formatted {len(formatted_tickers)} futures tickers for {base_coin}")
            # Не логировать первый элемент - экономия места в логе
            return formatted_tickers
        except Exception as e:
            logger.error(f"Error getting futures tickers for {base_coin}: {e}")
            logger.exception(e)  # Добавим полную трассировку исключения
            return None
            
    def get_spot_tickers(self, symbols: List[str]) -> Optional[List[Dict]]:
        """
        Получить тикерные данные для спотовых пар
        """
        try:
            endpoint = self.config['bybit_api']['endpoints']['tickers']
            
            # Получить список разрешенных quote coins из конфигурации
            allowed_quote_coins = self.config.get('futures', {}).get('quote_coins', ['USDT'])
            
            # Использовать первую quote монету из списка для спотовых пар
            primary_quote_coin = allowed_quote_coins[0] if allowed_quote_coins else 'USDT'
            
            # Process symbols to create spot pairs (e.g., BTC -> BTCUSDT)
            spot_symbols = [f"{symbol}{primary_quote_coin}" for symbol in symbols]
            logger.info(f"Fetching tickers for spot pairs: {spot_symbols}")
            
            # Получить список разрешенных тикеров из конфигурации
            allowed_tickers = self.config.get('tickers', [])
            
            all_tickers = []
            for symbol in spot_symbols:
                # Проверить, есть ли символ в списке разрешенных тикеров
                base_symbol = symbol.replace(primary_quote_coin, '')
                if base_symbol in allowed_tickers:
                    params = {
                        'category': 'spot',
                        'symbol': symbol
                    }
                    
                    data = self.get_bybit_api_data(endpoint, params)
                    
                    if data is None or data.get('retCode') != 0:
                        logger.warning(f"Failed to get spot ticker for {symbol}")
                        continue
                        
                    # Извлечь тикер из результата
                    ticker_list = data.get('result', {}).get('list', [])
                    if ticker_list:
                        ticker = ticker_list[0]  # Первый (и обычно единственный) элемент
                        formatted_ticker = {
                            'symbol': ticker.get('symbol'),
                            'lastPrice': ticker.get('lastPrice'),
                            'indexPrice': ticker.get('indexPrice'),
                            'markPrice': ticker.get('markPrice'),
                            'prevPrice24h': ticker.get('prevPrice24h'),
                            'price24hPcnt': ticker.get('price24hPcnt'),
                            'highPrice24h': ticker.get('highPrice24h'),
                            'lowPrice24h': ticker.get('lowPrice24h'),
                            'prevPrice1h': ticker.get('prevPrice1h'),
                            'openInterest': ticker.get('openInterest'),
                            'openInterestValue': ticker.get('openInterestValue'),
                            'turnover24h': ticker.get('turnover24h'),
                            'volume24h': ticker.get('volume24h'),
                            'usdIndexPrice': ticker.get('usdIndexPrice'),
                            # Добавлены данные для bid/ask
                            'bid1Price': ticker.get('bid1Price'),
                            'bid1Size': ticker.get('bid1Size'),
                            'ask1Price': ticker.get('ask1Price'),
                            'ask1Size': ticker.get('ask1Size')
                        }
                        all_tickers.append(formatted_ticker)
                    else:
                        logger.warning(f"No data found for spot ticker {symbol}")
            
            if not all_tickers:
                logger.warning(f"No spot tickers found for symbols {symbols}")
                return None
                
            return all_tickers
        except Exception as e:
            logger.error(f"Error getting spot tickers for symbols {symbols}: {e}")
            logger.exception(e)  # Добавим полную трассировку исключения
            return None

    def create_database_schema(self, db_path: str):
        """
        Создать схему базы данных для хранения всех данных Bybit
        """
        logger.debug(f"Creating database schema at {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создать таблицу Options_Data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS options_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_date TEXT NOT NULL,
                collection_time TEXT NOT NULL,
                base_coin TEXT NOT NULL,
                settle_coin TEXT,
                last_price REAL,
                index_price REAL
            )
        ''')
        
        # Создать таблицу Options_details
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS options_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id INTEGER,
                symbol TEXT,
                options_type TEXT,
                status TEXT,
                base_coin TEXT,
                quote_coin TEXT,
                settle_coin TEXT,
                launch_time TEXT,
                delivery_time TEXT,
                delivery_fee_rate TEXT,
                min_price TEXT,
                max_price TEXT,
                tick_size TEXT,
                max_order_qty TEXT,
                min_order_qty TEXT,
                qty_step TEXT,
                display_name TEXT,
                FOREIGN KEY (collection_id) REFERENCES options_data (id)
            )
        ''')
        
        # Создать таблицу options_tickers
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS options_tickers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id INTEGER,
                symbol TEXT,
                bid1_price REAL,
                bid1_size REAL,
                bid1_iv REAL,
                ask1_price REAL,
                ask1_size REAL,
                ask1_iv REAL,
                last_price REAL,
                high_price_24h REAL,
                low_price_24h REAL,
                mark_price REAL,
                index_price REAL,
                mark_iv REAL,
                underlying_price REAL,
                open_interest REAL,
                turnover_24h REAL,
                volume_24h REAL,
                total_volume REAL,
                total_turnover REAL,
                delta REAL,
                gamma REAL,
                vega REAL,
                theta REAL,
                predicted_delivery_price REAL,
                change_24h REAL,
                FOREIGN KEY (collection_id) REFERENCES options_data (id)
            )
        ''')
        
        # Создать таблицу futures_instruments
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS futures_instruments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id INTEGER,
                symbol TEXT,
                contract_type TEXT,
                status TEXT,
                base_coin TEXT,
                quote_coin TEXT,
                settle_coin TEXT,
                symbol_type TEXT,
                launch_time TEXT,
                delivery_time TEXT,
                delivery_fee_rate TEXT,
                price_scale TEXT,
                min_leverage TEXT,
                max_leverage TEXT,
                leverage_step TEXT,
                min_price TEXT,
                max_price TEXT,
                tick_size TEXT,
                min_notional_value TEXT,
                max_order_qty TEXT,
                max_mkt_order_qty TEXT,
                min_order_qty TEXT,
                qty_step TEXT,
                unified_margin_trade TEXT,
                funding_interval INTEGER,
                upper_funding_rate TEXT,
                lower_funding_rate TEXT,
                display_name TEXT,
                copy_trading TEXT,
                forbid_upl_withdrawal TEXT,
                FOREIGN KEY (collection_id) REFERENCES options_data (id)
            )
        ''')
        
        # Создать таблицу futures_data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS futures_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id INTEGER,
                symbol TEXT,
                last_price REAL,
                index_price REAL,
                mark_price REAL,
                prev_price_24h REAL,
                price_24h_pcnt REAL,
                high_price_24h REAL,
                low_price_24h REAL,
                prev_price_1h REAL,
                open_interest REAL,
                open_interest_value REAL,
                turnover_24h REAL,
                volume_24h REAL,
                funding_rate REAL,
                next_funding_time TEXT,
                predicted_delivery_price REAL,
                basis_rate REAL,
                basis REAL,
                delivery_fee_rate TEXT,
                delivery_time TEXT,
                bid1_price REAL,
                bid1_size REAL,
                ask1_price REAL,
                ask1_size REAL,
                pre_open_price REAL,
                pre_qty REAL,
                cur_pre_listing_phase TEXT,
                funding_interval_hour TEXT,
                funding_cap TEXT,
                basis_rate_year TEXT,
                FOREIGN KEY (collection_id) REFERENCES options_data (id)
            )
        ''')
        
        # Создать таблицу spot_data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS spot_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id INTEGER,
                symbol TEXT,
                bid1_price REAL,
                bid1_size REAL,
                ask1_price REAL,
                ask1_size REAL,
                last_price REAL,
                prev_price_24h REAL,
                price_24h_pcnt REAL,
                high_price_24h REAL,
                low_price_24h REAL,
                turnover_24h REAL,
                volume_24h REAL,
                usd_index_price REAL,
                FOREIGN KEY (collection_id) REFERENCES options_data (id)
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database schema created/updated at {db_path}")
        
    def save_all_data_to_db(self, base_coin: str, options_data: Optional[List[Dict]], 
                           options_tickers: Optional[List[Dict]], futures_instruments: Optional[List[Dict]], 
                           futures_tickers: Optional[List[Dict]], spot_data: Optional[List[Dict]]):
        """
        Сохранить все данные в базу данных SQLite
        """
        # Обработать None значения
        if options_data is None:
            options_data = []
        if options_tickers is None:
            options_tickers = []
        if futures_instruments is None:
            futures_instruments = []
        if futures_tickers is None:
            futures_tickers = []
        if spot_data is None:
            spot_data = []
        
        logger.debug(f"Saving data to database for {base_coin}:")
        logger.debug(f"  Options data: {len(options_data)} items")
        logger.debug(f"  Options tickers: {len(options_tickers)} items")
        logger.debug(f"  Futures instruments: {len(futures_instruments)} items")
        logger.debug(f"  Futures tickers: {len(futures_tickers)} items")
        logger.debug(f"  Spot data: {len(spot_data)} items")
        
        # Дополнительное логирование для SOL (только количество)
        if base_coin == 'SOL':
            logger.debug(f"SOL DB SAVE: Options={len(options_data)}, Tickers={len(options_tickers)}")
        
        # Получить путь к базе данных для этого тикера
        data_folder = self.config.get('data_folder', './bybit_data')
        # Убедиться, что путь к папке данных не содержит обратных слешей, которые могут вызвать проблемы
        data_folder = data_folder.replace('\\', '/').replace('\\', '/')
        db_path = os.path.join(data_folder, f"Bybit_{base_coin}", f"Bybit_{base_coin}_API.db").replace('\\', '/').replace('\\', '/')
        
        logger.debug(f"Database path: {db_path}")
        
        # Создать схему базы данных, если она не существует
        self.create_database_schema(db_path)
        
        # Соединиться с базой данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получить текущую дату и время
        now = datetime.datetime.now()
        collection_date = now.strftime('%Y-%m-%d')
        collection_time = now.strftime('%H:%M:%S')
        
        # Получить settleCoin из futures_instruments (если доступно)
        settle_coin = None
        if futures_instruments and len(futures_instruments) > 0:
            settle_coin = futures_instruments[0].get('settleCoin')
        
        # Получить last_price и index_price из spot_data и futures_data соответственно
        last_price = None
        index_price = None
        if spot_data and len(spot_data) > 0:
            last_price_str = spot_data[0].get('lastPrice')
            if last_price_str not in (None, ''):
                try:
                    last_price = float(last_price_str)
                except (ValueError, TypeError):
                    last_price = None
        
        # Исправлено: выбрать index_price из фьючерса основного тикера плюс USDT
        # Улучшена фильтрация для точного выбора фьючерса основной монеты
        if futures_tickers and len(futures_tickers) > 0:
            target_symbol = f"{base_coin}USDT"
            index_price_str = None
            selected_ticker_symbol = None
            
            # Найти фьючерс с символом основного тикера плюс USDT
            # Уточненная логика: ищем точное совпадение с {base_coin}USDT, 
            # но также проверяем, что это не часть другого символа
            for ticker in futures_tickers:
                symbol = ticker.get('symbol', '')
                # Проверяем точное совпадение с {base_coin}USDT
                if symbol == target_symbol:
                    index_price_str = ticker.get('indexPrice')
                    selected_ticker_symbol = ticker.get('symbol')
                    logger.debug(f"Выбран фьючерс {selected_ticker_symbol} для получения index_price: {index_price_str}")
                    break
            
            # Если не нашли точное совпадение, ищем альтернативные варианты
            if index_price_str is None:
                logger.debug(f"Точное совпадение {target_symbol} не найдено, ищем альтернативы")
                for ticker in futures_tickers:
                    symbol = ticker.get('symbol', '')
                    # Проверяем, начинается ли символ с {base_coin}USDT и имеет дополнительный суффикс
                    if symbol.startswith(target_symbol) and len(symbol) > len(target_symbol):
                        # Это фьючерс с датой экспирации, например SOLUSDT-14NOV25
                        index_price_str = ticker.get('indexPrice')
                        selected_ticker_symbol = ticker.get('symbol')
                        logger.debug(f"Выбран фьючерс с датой экспирации {selected_ticker_symbol} для получения index_price: {index_price_str}")
                        break
            
            # Если до сих пор не нашли, используем первый элемент как fallback
            if index_price_str is None:
                index_price_str = futures_tickers[0].get('indexPrice')
                selected_ticker_symbol = futures_tickers[0].get('symbol') if futures_tickers else 'Unknown'
                logger.warning(f"Не найден подходящий фьючерс {target_symbol} для получения index_price, используем первый элемент: {selected_ticker_symbol}")
            
            if index_price_str not in (None, ''):
                try:
                    index_price = float(index_price_str)
                    logger.debug(f"Установлен index_price из фьючерса {selected_ticker_symbol}: {index_price}")
                except (ValueError, TypeError):
                    index_price = None
                    logger.error(f"Ошибка преобразования index_price в число: {index_price_str}")
            else:
                logger.warning(f"Пустое значение index_price в фьючерсе {selected_ticker_symbol}")
        
        # Вставить основные данные в таблицу options_data
        cursor.execute('''
            INSERT INTO options_data (
                collection_date, collection_time, base_coin, settle_coin, last_price, index_price
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            collection_date, collection_time, base_coin, settle_coin, last_price, index_price
        ))
        
        # Получить ID вставленной строки
        collection_id = cursor.lastrowid
        logger.debug(f"Inserted main record with collection_id: {collection_id}")
        
        # Вставить детали опционов в таблицу options_details
        if options_data:
            for option in options_data:
                cursor.execute('''
                    INSERT INTO options_details (
                        collection_id, symbol, options_type, status, base_coin,
                        quote_coin, settle_coin, launch_time, delivery_time,
                        delivery_fee_rate, min_price, max_price, tick_size,
                        max_order_qty, min_order_qty, qty_step, display_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    collection_id,
                    option.get('symbol'), option.get('optionsType'), option.get('status'),
                    option.get('baseCoin'), option.get('quoteCoin'), option.get('settleCoin'),
                    option.get('launchTime'), option.get('deliveryTime'),
                    option.get('deliveryFeeRate'),
                    option.get('minPrice'), option.get('maxPrice'), option.get('tickSize'),
                    option.get('maxOrderQty'), option.get('minOrderQty'), option.get('qtyStep'),
                    option.get('displayName')
                ))
                
        # Вставить тикерные данные опционов в таблицу options_tickers
        if options_tickers:
            for ticker in options_tickers:
                cursor.execute('''
                    INSERT INTO options_tickers (
                        collection_id, symbol, bid1_price, bid1_size, bid1_iv,
                        ask1_price, ask1_size, ask1_iv, last_price, high_price_24h,
                        low_price_24h, mark_price, index_price, mark_iv, underlying_price,
                        open_interest, turnover_24h, volume_24h, total_volume,
                        total_turnover, delta, gamma, vega, theta,
                        predicted_delivery_price, change_24h
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    collection_id,
                    ticker.get('symbol'),
                    ticker.get('bid1Price') if ticker.get('bid1Price') not in (None, '') else None,
                    ticker.get('bid1Size') if ticker.get('bid1Size') not in (None, '') else None,
                    ticker.get('bid1Iv') if ticker.get('bid1Iv') not in (None, '') else None,
                    ticker.get('ask1Price') if ticker.get('ask1Price') not in (None, '') else None,
                    ticker.get('ask1Size') if ticker.get('ask1Size') not in (None, '') else None,
                    ticker.get('ask1Iv') if ticker.get('ask1Iv') not in (None, '') else None,
                    ticker.get('lastPrice') if ticker.get('lastPrice') not in (None, '') else None,
                    ticker.get('highPrice24h') if ticker.get('highPrice24h') not in (None, '') else None,
                    ticker.get('lowPrice24h') if ticker.get('lowPrice24h') not in (None, '') else None,
                    ticker.get('markPrice') if ticker.get('markPrice') not in (None, '') else None,
                    ticker.get('indexPrice') if ticker.get('indexPrice') not in (None, '') else None,
                    ticker.get('markIv') if ticker.get('markIv') not in (None, '') else None,
                    ticker.get('underlyingPrice') if ticker.get('underlyingPrice') not in (None, '') else None,
                    ticker.get('openInterest') if ticker.get('openInterest') not in (None, '') else None,
                    ticker.get('turnover24h') if ticker.get('turnover24h') not in (None, '') else None,
                    ticker.get('volume24h') if ticker.get('volume24h') not in (None, '') else None,
                    ticker.get('totalVolume') if ticker.get('totalVolume') not in (None, '') else None,
                    ticker.get('totalTurnover') if ticker.get('totalTurnover') not in (None, '') else None,
                    ticker.get('delta') if ticker.get('delta') not in (None, '') else None,
                    ticker.get('gamma') if ticker.get('gamma') not in (None, '') else None,
                    ticker.get('vega') if ticker.get('vega') not in (None, '') else None,
                    ticker.get('theta') if ticker.get('theta') not in (None, '') else None,
                    ticker.get('predictedDeliveryPrice') if ticker.get('predictedDeliveryPrice') not in (None, '') else None,
                    ticker.get('change24h') if ticker.get('change24h') not in (None, '') else None
                ))
        
        # Вставить данные фьючерсных инструментов в таблицу futures_instruments
        if futures_instruments:
            logger.info(f"Saving {len(futures_instruments)} futures instruments to database")
            for i, future in enumerate(futures_instruments):
                try:
                    cursor.execute('''
                        INSERT INTO futures_instruments (
                            collection_id, symbol, contract_type, status, base_coin,
                            quote_coin, settle_coin, symbol_type, launch_time, delivery_time,
                            delivery_fee_rate, price_scale, min_leverage, max_leverage, leverage_step,
                            min_price, max_price, tick_size, min_notional_value,
                            max_order_qty, max_mkt_order_qty, min_order_qty, qty_step,
                            unified_margin_trade, funding_interval, upper_funding_rate, lower_funding_rate,
                            display_name, copy_trading, forbid_upl_withdrawal
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        collection_id,
                        future.get('symbol'), future.get('contractType'), future.get('status'),
                        future.get('baseCoin'), future.get('quoteCoin'), future.get('settleCoin'),
                        future.get('symbolType'), future.get('launchTime'), future.get('deliveryTime'),
                        future.get('deliveryFeeRate'), future.get('priceScale'),
                        future.get('minLeverage'), future.get('maxLeverage'), future.get('leverageStep'),
                        future.get('minPrice'), future.get('maxPrice'), future.get('tickSize'),
                        future.get('minNotionalValue'),
                        future.get('maxOrderQty'), future.get('maxMktOrderQty'), future.get('minOrderQty'), future.get('qtyStep'),
                        str(future.get('unifiedMarginTrade')), future.get('fundingInterval'),
                        future.get('upperFundingRate'), future.get('lowerFundingRate'),
                        future.get('displayName'), future.get('copyTrading'), str(future.get('forbidUplWithdrawal'))
                    ))
                except Exception as e:
                    logger.error(f"Error inserting futures instrument {future.get('symbol', 'Unknown')}: {e}")
                
        # Вставить тикерные данные фьючерсов в таблицу futures_data
        if futures_tickers:
            logger.info(f"Saving {len(futures_tickers)} futures tickers to database")
            for i, ticker in enumerate(futures_tickers):
                try:
                    cursor.execute('''
                        INSERT INTO futures_data (
                            collection_id, symbol, last_price, index_price, mark_price,
                            prev_price_24h, price_24h_pcnt, high_price_24h, low_price_24h, prev_price_1h,
                            open_interest, open_interest_value, turnover_24h, volume_24h, funding_rate,
                            next_funding_time, predicted_delivery_price, basis_rate, basis, delivery_fee_rate, delivery_time,
                            bid1_price, bid1_size, ask1_price, ask1_size, pre_open_price,
                            pre_qty, cur_pre_listing_phase, funding_interval_hour, funding_cap, basis_rate_year
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        collection_id,
                        ticker.get('symbol'),
                        ticker.get('lastPrice') if ticker.get('lastPrice') not in (None, '') else None,
                        ticker.get('indexPrice') if ticker.get('indexPrice') not in (None, '') else None,
                        ticker.get('markPrice') if ticker.get('markPrice') not in (None, '') else None,
                        ticker.get('prevPrice24h') if ticker.get('prevPrice24h') not in (None, '') else None,
                        ticker.get('price24hPcnt') if ticker.get('price24hPcnt') not in (None, '') else None,
                        ticker.get('highPrice24h') if ticker.get('highPrice24h') not in (None, '') else None,
                        ticker.get('lowPrice24h') if ticker.get('lowPrice24h') not in (None, '') else None,
                        ticker.get('prevPrice1h') if ticker.get('prevPrice1h') not in (None, '') else None,
                        ticker.get('openInterest') if ticker.get('openInterest') not in (None, '') else None,
                        ticker.get('openInterestValue') if ticker.get('openInterestValue') not in (None, '') else None,
                        ticker.get('turnover24h') if ticker.get('turnover24h') not in (None, '') else None,
                        ticker.get('volume24h') if ticker.get('volume24h') not in (None, '') else None,
                        ticker.get('fundingRate') if ticker.get('fundingRate') not in (None, '') else None,
                        ticker.get('nextFundingTime'),
                        ticker.get('predictedDeliveryPrice') if ticker.get('predictedDeliveryPrice') not in (None, '') else None,
                        ticker.get('basisRate') if ticker.get('basisRate') not in (None, '') else None,
                        ticker.get('basis') if ticker.get('basis') not in (None, '') else None,
                        ticker.get('deliveryFeeRate'),
                        ticker.get('deliveryTime'),
                        ticker.get('bid1Price') if ticker.get('bid1Price') not in (None, '') else None,
                        ticker.get('bid1Size') if ticker.get('bid1Size') not in (None, '') else None,
                        ticker.get('ask1Price') if ticker.get('ask1Price') not in (None, '') else None,
                        ticker.get('ask1Size') if ticker.get('ask1Size') not in (None, '') else None,
                        ticker.get('preOpenPrice') if ticker.get('preOpenPrice') not in (None, '') else None,
                        ticker.get('preQty') if ticker.get('preQty') not in (None, '') else None,
                        ticker.get('curPreListingPhase'),
                        ticker.get('fundingIntervalHour'),
                        ticker.get('fundingCap'),
                        ticker.get('basisRateYear')
                    ))
                    logger.debug(f"Successfully inserted futures ticker #{i+1}: {ticker.get('symbol', 'Unknown')}")
                except Exception as e:
                    logger.error(f"Error inserting futures ticker #{i+1} ({ticker.get('symbol', 'Unknown')}): {e}")
                    logger.exception(e)
                    # Продолжить вставку остальных записей
                    continue
        else:
            logger.info("No futures tickers to save to database")
        
        # Вставить спотовые данные в таблицу spot_data
        if spot_data:
            for ticker in spot_data:
                cursor.execute('''
                    INSERT INTO spot_data (
                        collection_id, symbol, bid1_price, bid1_size,
                        ask1_price, ask1_size, last_price, prev_price_24h,
                        price_24h_pcnt, high_price_24h, low_price_24h,
                        turnover_24h, volume_24h, usd_index_price
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    collection_id,
                    ticker.get('symbol'),
                    ticker.get('bid1Price') if ticker.get('bid1Price') not in (None, '') else None,
                    ticker.get('bid1Size') if ticker.get('bid1Size') not in (None, '') else None,
                    ticker.get('ask1Price') if ticker.get('ask1Price') not in (None, '') else None,
                    ticker.get('ask1Size') if ticker.get('ask1Size') not in (None, '') else None,
                    ticker.get('lastPrice') if ticker.get('lastPrice') not in (None, '') else None,
                    ticker.get('prevPrice24h') if ticker.get('prevPrice24h') not in (None, '') else None,
                    ticker.get('price24hPcnt') if ticker.get('price24hPcnt') not in (None, '') else None,
                    ticker.get('highPrice24h') if ticker.get('highPrice24h') not in (None, '') else None,
                    ticker.get('lowPrice24h') if ticker.get('lowPrice24h') not in (None, '') else None,
                    ticker.get('turnover24h') if ticker.get('turnover24h') not in (None, '') else None,
                    ticker.get('volume24h') if ticker.get('volume24h') not in (None, '') else None,
                    ticker.get('usdIndexPrice') if ticker.get('usdIndexPrice') not in (None, '') else None
                ))
            
            try:
                conn.commit()
                logger.info("Database commit successful")
                
                # Проверить, сколько записей было вставлено в futures_data
                cursor.execute("SELECT COUNT(*) FROM futures_data WHERE collection_id = ?", (collection_id,))
                futures_count = cursor.fetchone()[0]
                logger.info(f"Inserted {futures_count} records into futures_data table")
            except Exception as e:
                logger.error(f"Error committing to database: {e}")
                logger.exception(e)
            finally:
                conn.close()
                logger.info(f"Saved all data for {base_coin} to database")

    def collect_data_for_ticker(self, base_coin: str):
        """
        Собрать все необходимые данные для конкретной монеты
        Возвращает количество собранных опционов
        """
        try:
            logger.info(f"Starting data collection for {base_coin}")
            
            # 1. Получить данные опционов
            logger.debug(f"Getting options data for {base_coin}")
            options_data = self.get_options_data(base_coin)
            logger.debug(f"Options data result: {type(options_data)}, length: {len(options_data) if options_data else 0}")
            
            # 2. Получить тикерные данные опционов
            logger.debug(f"Getting options tickers for {base_coin}")
            options_tickers = self.get_options_tickers(base_coin)
            logger.debug(f"Options tickers result: {type(options_tickers)}, length: {len(options_tickers) if options_tickers else 0}")
            
            # 3. Получить данные фьючерсных инструментов
            logger.debug(f"Getting futures instruments for {base_coin}")
            futures_instruments = self.get_futures_instruments(base_coin)
            logger.debug(f"Futures instruments result: {type(futures_instruments)}, length: {len(futures_instruments) if futures_instruments else 0}")
            
            # 4. Получить тикерные данные фьючерсов
            logger.debug(f"Getting futures tickers for {base_coin}")
            futures_tickers = self.get_futures_tickers(base_coin)
            logger.debug(f"Futures tickers result: {type(futures_tickers)}, length: {len(futures_tickers) if futures_tickers else 0}")
            
            # 5. Получить спотовые данные
            logger.debug(f"Getting spot tickers for {base_coin}")
            spot_data = self.get_spot_tickers([base_coin])
            logger.debug(f"Spot data result: {type(spot_data)}, length: {len(spot_data) if spot_data else 0}")
            
            # Добавим отладочную информацию
            logger.info(f"Collected data for {base_coin}:")
            logger.info(f"  Options data: {len(options_data) if options_data else 0} items")
            logger.info(f"  Options tickers: {len(options_tickers) if options_tickers else 0} items")
            logger.info(f"  Futures instruments: {len(futures_instruments) if futures_instruments else 0} items")
            logger.info(f"  Futures tickers: {len(futures_tickers) if futures_tickers else 0} items")
            logger.info(f"  Spot data: {len(spot_data) if spot_data else 0} items")
            
            # Добавим отладочную информацию о фьючерсах
            if futures_tickers:
                logger.debug(f"First futures ticker: {futures_tickers[0] if futures_tickers else 'None'}")
            if futures_instruments:
                logger.debug(f"First futures instrument: {futures_instruments[0] if futures_instruments else 'None'}")
            
            # 6. Обработать None значения перед сохранением
            if options_data is None:
                options_data = []  # Передать пустой список вместо None
                logger.warning(f"No options data received for {base_coin}, using empty list")
                
            if options_tickers is None:
                options_tickers = []  # Передать пустой список вместо None
                logger.warning(f"No options tickers data received for {base_coin}, using empty list")
                
            if futures_instruments is None:
                futures_instruments = []  # Передать пустой список вместо None
                logger.warning(f"No futures instruments data received for {base_coin}, using empty list")
                
            if futures_tickers is None:
                futures_tickers = []  # Передать пустой список вместо None
                logger.warning(f"No futures tickers data received for {base_coin}, using empty list")
                
            if spot_data is None:
                spot_data = []  # Передать пустой список вместо None
                logger.warning(f"No spot data received for {base_coin}, using empty list")
            
            # Логирование для SOL (сокращенно)
            if base_coin == 'SOL':
                logger.debug(f"SOL: options={len(options_data) if options_data else 0}, tickers={len(options_tickers) if options_tickers else 0}")
            
            # Сохранить количество собранных опционов ДО попытки сохранения в БД
            options_count = len(options_data) if options_data else 0
            
            # 7. Сохранить данные в базу данных
            logger.debug(f"Saving all data to database for {base_coin}")
            try:
                self.save_all_data_to_db(base_coin, options_data, options_tickers, 
                                       futures_instruments, futures_tickers, spot_data)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    logger.warning(f"Database is locked for {base_coin}, but data was collected. Error: {e}")
                    # Продолжаем выполнение, так как данные уже собраны
                else:
                    logger.error(f"Database error for {base_coin}: {e}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error saving data for {base_coin}: {e}")
                logger.exception(e)
                # Продолжаем выполнение, так как данные уже собраны
            
            logger.info(f"Completed data collection for {base_coin}. Options collected: {options_count}")
            
            # Логирование результата для SOL
            if base_coin == 'SOL':
                logger.debug(f"SOL FINAL: {options_count} options")
                
            return options_count
        except Exception as e:
            logger.error(f"Error collecting data for {base_coin}: {e}")
            logger.exception(e)  # Добавим полную трассировку исключения
            return 0

    def collect_data(self):
        """
        Основной метод для сбора данных по всем тикерам
        Возвращает True, если данные были собраны, False в противном случае
        """
        print("=== Starting Bybit Data Collection ===")
        logger.info("=== Starting Bybit Data Collection ===")
        
        # Залогировать текущую дату и время
        start_time = datetime.datetime.now()
        
        # Проверить, является ли сегодня торговым днем
        is_trading_day = self.is_bybit_trading_day()
        
        if not is_trading_day:
            print("Today is not a trading day. Skipping data collection.")
            print("To override this for testing, modify the trading_days setting in Config_API_Bybit.yaml")
            logger.info("Today is not a trading day. Skipping data collection.")
            return False  # Return False to indicate no data was collected
            
        # Проверить, открыта ли биржа, если включена опция market_hours_only
        market_hours_only = self.config.get('collection', {}).get('market_hours_only', True)
        if market_hours_only and not self.is_market_open():
            print("Market is closed. Skipping data collection.")
            logger.info("Market is closed. Skipping data collection.")
            return False  # Return False to indicate no data was collected
            
        # Собрать данные для каждого тикера
        tickers = self.config.get('tickers', [])
        
        if not tickers:
            print("No tickers found in configuration. Please check Config_API_Bybit.yaml")
            logger.warning("No tickers found in configuration. Please check Config_API_Bybit.yaml")
            return False
            
        # Отследить результаты сбора
        results = {}
        errors = []
        
        print(f"Collecting data for {len(tickers)} tickers...")
        logger.info(f"Collecting data for {len(tickers)} tickers: {tickers}")
        spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        spinner_index = 0
        
        for i, ticker in enumerate(tickers):
            try:
                # Показать прогресс со спиннером
                print(f"\r{spinner[spinner_index % len(spinner)]} Processing {ticker}...", end="", flush=True)
                logger.info(f"Processing ticker {ticker} ({i+1}/{len(tickers)})")
                spinner_index += 1
                
                # Собрать данные для тикера и отследить, сколько опционов было собрано
                options_collected = self.collect_data_for_ticker(ticker)
                
                # Сохранить количество собранных опционов
                results[ticker] = options_collected if options_collected is not None else 0
                    
                # Обновить прогресс
                print(f"\r✓ Completed {ticker} ({results[ticker]} options)", flush=True)
                logger.info(f"Completed {ticker} ({results[ticker]} options)")
                    
            except Exception as e:
                errors.append(f"{ticker}: {str(e)}")
                logger.error(f"Error processing ticker {ticker}: {e}")
                logger.exception(e)
                
        # Рассчитать время выполнения
        end_time = datetime.datetime.now()
        execution_time = end_time - start_time
        
        # Напечатать сводку
        print("\n" + "=" * 50)
        print("BYBIT DATA COLLECTION SUMMARY")
        print("=" * 50)
        for ticker, count in results.items():
            print(f"{ticker:8}: {count:4} options")
        print("-" * 50)
        print(f"Execution time: {execution_time}")
        print("=" * 50)
        
        # Напечатать ошибки, если есть
        if errors:
            print("\nERRORS OCCURRED:")
            for error in errors:
                print(f"  - {error}")
            print("\nPlease check the log file for detailed error information.")
            logger.error("ERRORS OCCURRED:")
            for error in errors:
                logger.error(f"  - {error}")
        else:
            print("\nData collection completed successfully!")
            logger.info("Data collection completed successfully!")
            
        return True  # Return True to indicate data was collected

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(sig, frame):
    global shutdown_requested
    print('\nShutdown requested...')
    shutdown_requested = True

def main():
    """
    Основная функция для запуска сборщика данных Bybit
    """
    logger.debug("Starting main function")
    
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    collector = BybitDataCollector()
    
    # Get scheduling configuration
    schedule_interval = collector.config.get('collection', {}).get('schedule_interval', 0)
    auto_stop_at_close = collector.config.get('collection', {}).get('auto_stop_at_close', True)
    market_hours_only = collector.config.get('collection', {}).get('market_hours_only', True)
    
    logger.debug(f"Scheduling configuration: schedule_interval={schedule_interval}, auto_stop_at_close={auto_stop_at_close}, market_hours_only={market_hours_only}")
    
    # Initialize counters and tracking variables
    calculation_count = 0
    last_calculation_date = None
    
    if schedule_interval <= 0:
        # Run once
        logger.info("Running data collection once")
        if not market_hours_only or collector.is_market_open():
            collector.collect_data()
        else:
            print("Market is closed. Data collection not performed.")
            logger.info("Market is closed. Data collection not performed.")
    else:
        # Run periodically
        print(f"Starting periodic data collection every {schedule_interval} minutes")
        print("Press Ctrl+C to exit")
        logger.info(f"Starting periodic data collection every {schedule_interval} minutes")
        global shutdown_requested
        while not shutdown_requested:
            # Check if we should only run during market hours
            if market_hours_only and not collector.is_market_open():
                if auto_stop_at_close:
                    print("Market is closed. Exiting.")
                    logger.info("Market is closed. Exiting.")
                    break
                else:
                    print("Market is closed. Waiting for market to open...")
                    logger.info("Market is closed. Waiting for market to open...")
                    # Wait 10 minutes before checking again
                    for _ in range(60):  # Check every minute for 10 minutes
                        if shutdown_requested:
                            break
                        time.sleep(10)
                    continue
            
            # Get current date and time for collection
            now = datetime.datetime.now()
            collection_date = now.strftime("%Y-%m-%d")
            collection_time = now.strftime("%H:%M:%S")
            
            # Reset calculation count if it's a new day
            if last_calculation_date != collection_date:
                calculation_count = 0
                last_calculation_date = collection_date
            
            # Increment calculation counter
            calculation_count += 1
            
            # Display calculation information
            print(f"\n[{collection_date} {collection_time}] Starting data collection cycle #{calculation_count} for today")
            logger.info(f"[{collection_date} {collection_time}] Starting data collection cycle #{calculation_count} for today")
            
            # Collect data
            collector.collect_data()
            
            if shutdown_requested:
                break
                
            # Calculate next calculation time
            next_calculation = now + datetime.timedelta(minutes=schedule_interval)
            next_calculation_time = next_calculation.strftime("%Y-%m-%d %H:%M:%S")
                
            # Wait for next execution
            print(f"Waiting {schedule_interval} minutes until next collection...")
            print(f"Next collection scheduled for: {next_calculation_time}")
            logger.info(f"Waiting {schedule_interval} minutes until next collection...")
            logger.info(f"Next collection scheduled for: {next_calculation_time}")
            for _ in range(schedule_interval * 60):  # Check every second
                if shutdown_requested:
                    break
                time.sleep(1)
                
        print("Data collection finished.")
        logger.info("Data collection finished.")

if __name__ == "__main__":
    main()