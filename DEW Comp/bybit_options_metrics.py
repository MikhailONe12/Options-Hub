import sqlite3
import yaml
import os
import logging
from logging.handlers import RotatingFileHandler
import time
import math
import datetime
import threading
import sys
from functools import wraps

# Настройка логирования с ротацией файлов
log_handler = RotatingFileHandler(
    'bybit_options_metrics.log',
    maxBytes=4_194_304,  # 4 MB
    backupCount=1,  # Только 1 резервная копия (текущий + 1 старый)
    encoding='utf-8'
)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler]
)
logger = logging.getLogger(__name__)


def timer_decorator(func):
    """Decorator to log execution time of functions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        logger.info(f"{func.__name__} executed in {elapsed_time:.2f} seconds")
        return result
    return wrapper


class OptionsAnalyzer:
    def __init__(self, config_path):
        # Determine the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # If config_path is a relative path, make it relative to the script directory
        if not os.path.isabs(config_path):
            config_path = os.path.join(script_dir, config_path)
        
        self.config_path = config_path
        self.config = self.load_config()
        self.data_folder = self.config.get('data_folder', './bybit_data')
        self.metrics_folder = self.config.get('metrics_folder', './bybit_metrics')
        self._db_connections = {}  # Cache for database connections
        self._ivr_cache = {}  # Cache for IVR calculations
        self._enhanced_ivr_cache = {}  # Cache for enhanced IVR calculations

    def load_config(self):
        """
        Загрузить конфигурацию из файла
        """
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"Config file not found: {self.config_path}")
            return {}
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return {}

    def connect_db(self, db_path):
        """
        Подключиться к базе данных с кэшированием соединений
        """
        try:
            # Check cache first
            if db_path in self._db_connections:
                conn = self._db_connections[db_path]
                # Test if connection is still valid
                try:
                    conn.execute('SELECT 1')
                    return conn
                except:
                    # Connection is dead, remove from cache
                    del self._db_connections[db_path]
            
            # Create new connection with optimized settings
            conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            # Optimize connection settings
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=10000')
            conn.execute('PRAGMA query_only=False')
            
            # Cache the connection
            self._db_connections[db_path] = conn
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database {db_path}: {e}")
            return None

    def fetch_latest_options_data(self, symbol):
        """
        Получить последние данные опционов из базы данных Bybit
        """
        try:
            # Путь к базе данных для символа
            db_path = os.path.join(self.data_folder, f"Bybit_{symbol}", f"Bybit_{symbol}_API.db")
            
            # Check if file exists
            if not os.path.exists(db_path):
                logger.error(f"Database file not found: {db_path}")
                return None, None, None
                
            # Подключиться к базе данных
            conn = self.connect_db(db_path)
            if conn is None:
                return None, None, None
                
            cursor = conn.cursor()
            
            # Получить последний collection_id
            cursor.execute('SELECT MAX(id) FROM options_data')
            latest_collection_id = cursor.fetchone()[0]
            
            if latest_collection_id is None:
                logger.warning(f"No data found in {db_path}")
                conn.close()
                return None, None, None
                
            # Get latest collection info
            cursor.execute('SELECT * FROM options_data WHERE id = ?', (latest_collection_id,))
            options_data = cursor.fetchone()
            
            # Получить детали опционов
            cursor.execute('SELECT * FROM options_details WHERE collection_id = ?', (latest_collection_id,))
            options_details = cursor.fetchall()
            
            # Получить тикерные данные
            cursor.execute('SELECT * FROM options_tickers WHERE collection_id = ?', (latest_collection_id,))
            options_tickers = cursor.fetchall()
            
            conn.close()
            
            return options_data, options_details, options_tickers
        except Exception as e:
            logger.error(f"Error fetching latest options data for {symbol}: {e}")
            return None, None, None

    def fetch_latest_spot_price(self, symbol):
        """
        Получить последнюю спотовую цену из базы данных Bybit
        """
        try:
            # Путь к базе данных для символа
            db_path = os.path.join(self.data_folder, f"Bybit_{symbol}", f"Bybit_{symbol}_API.db")
            
            # Check if file exists
            if not os.path.exists(db_path):
                logger.error(f"Database file not found: {db_path}")
                return 0.0
                
            # Подключиться к базе данных
            conn = self.connect_db(db_path)
            if conn is None:
                return 0.0
                
            cursor = conn.cursor()
            
            # Получить последний collection_id
            cursor.execute('SELECT MAX(id) FROM options_data')
            latest_collection_id = cursor.fetchone()[0]
            
            if latest_collection_id is None:
                logger.warning(f"No data found in {db_path}")
                conn.close()
                return 0.0
                
            # Получить последнюю спотовую цену из тикерных данных
            cursor.execute('''
                SELECT underlying_price FROM options_tickers 
                WHERE collection_id = ? AND underlying_price > 0 
                ORDER BY id DESC 
                LIMIT 1
            ''', (latest_collection_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0] is not None:
                return float(result[0])
            else:
                logger.warning(f"No spot price found for {symbol}")
                return 0.0
                
        except Exception as e:
            logger.error(f"Error fetching latest spot price for {symbol}: {e}")
            return 0.0

    def calculate_metrics(self, options_data, options_details, options_tickers):
        """
        Рассчитать опционные метрики
        """
        try:
            # Extract underlying asset price (take from latest ticker)
            underlying_price = 0
            if options_tickers:
                # Find ticker with non-zero underlying price
                for ticker in options_tickers:
                    # Check that index 15 exists and value is not None
                    if len(ticker) > 15 and ticker[15] is not None and ticker[15] > 0:  # underlying_price
                        underlying_price = float(ticker[15])
                        break
            
            # Если не удалось получить цену базового актива, попробуем альтернативные методы
            if underlying_price == 0:
                # Попробуем получить из других полей или рассчитать
                for ticker in options_tickers:
                    if len(ticker) > 20 and ticker[20] is not None and ticker[20] > 0:  # Возможно, цена в другом поле
                        underlying_price = float(ticker[20])
                        break
            
            # Рассчитать греки (упрощенно)
            total_delta = 0
            total_gamma = 0
            total_vega = 0
            total_theta = 0
            total_rho = 0  # Rho
            
            # Greeks by option type
            call_delta = 0
            put_delta = 0
            call_gamma = 0
            put_gamma = 0
            
            # GEX (Gamma Exposure) - инициализация переменной для накопления GEX
            gex = 0
            
            # DEX (Delta Exposure)
            dex = 0
            
            # List to store individual contract GEX/DEX values
            contract_gex_dex_data = []
            
            # Греки второго порядка
            vanna = 0  # Ванна
            charm = 0  # Чарм
            vomma = 0  # Вомма/Volga
            veta = 0   # Вета
            
            # Греки третьего порядка
            speed = 0  # Скорость
            color = 0  # Цвет
            ultima = 0 # Ультима
            zomma = 0  # Зомма
            
            # Option value metrics
            intrinsic_value = 0  # Intrinsic value
            extrinsic_value = 0  # Extrinsic value
            option_premium = 0   # Option premium
            moneyness = 0        # Moneyness
            bid_ask_spread = 0   # Bid-Ask spread
            
            # Метрики волатильности
            implied_volatility = 0  # Подразумеваемая волатильность
            historical_volatility = 0  # Историческая волатильность
            realized_volatility = 0    # Реализованная волатильность
            volatility_skew_metric = 0 # Волатильность асимметрии
            term_structure = 0         # Термин структура волатильности
            
            # Distribution metrics
            skewness = 0  # Skewness
            kurtosis = 0  # Kurtosis
            
            # Рыночные метрики
            put_call_ratio = 0  # Put-Call Ratio
            open_interest_total = 0  # Open Interest
            volume_total = 0         # Volume
            market_depth = 0         # Market depth
            order_imbalance = 0      # Order imbalance
            order_flow = 0           # Order flow
            
            # Сбор данных для улыбки волатильности
            strikes = []
            ivs = []
            call_ivs = []
            put_ivs = []
            
            call_count = 0
            put_count = 0
            call_volume = 0
            put_volume = 0
            call_open_interest = 0
            put_open_interest = 0
            
            # Списки для расчета статистических метрик
            all_deltas = []
            all_gammas = []
            all_vegas = []
            all_thetas = []
            all_ivs = []
            
            valid_tickers = 0  # Счетчик валидных тикеров
            
            for ticker in options_tickers:
                # Check tuple length before accessing elements
                if len(ticker) < 26:
                    continue
                    
                # Извлечь необходимые данные
                symbol = str(ticker[2]) if len(ticker) > 2 and ticker[2] is not None else ""
                bid_price = float(ticker[3]) if len(ticker) > 3 and ticker[3] is not None else 0
                bid_size = float(ticker[4]) if len(ticker) > 4 and ticker[4] is not None else 0
                ask_price = float(ticker[6]) if len(ticker) > 6 and ticker[6] is not None else 0
                ask_size = float(ticker[7]) if len(ticker) > 7 and ticker[7] is not None else 0
                last_price = float(ticker[9]) if len(ticker) > 9 and ticker[9] is not None else 0
                volume = float(ticker[18]) if len(ticker) > 18 and ticker[18] is not None else 0
                open_interest = float(ticker[16]) if len(ticker) > 16 and ticker[16] is not None else 0
                delta = float(ticker[21]) if len(ticker) > 21 and ticker[21] is not None else 0
                gamma = float(ticker[22]) if len(ticker) > 22 and ticker[22] is not None else 0
                vega = float(ticker[23]) if len(ticker) > 23 and ticker[23] is not None else 0
                theta = float(ticker[24]) if len(ticker) > 24 and ticker[24] is not None else 0
                bid_iv = float(ticker[5]) if len(ticker) > 5 and ticker[5] is not None else 0
                ask_iv = float(ticker[8]) if len(ticker) > 8 and ticker[8] is not None else 0
                
                # Проверим, есть ли хоть какие-то данные по этому тикеру
                if (bid_price > 0 or ask_price > 0 or last_price > 0 or 
                    delta != 0 or gamma != 0 or vega != 0 or theta != 0):
                    valid_tickers += 1
                else:
                    continue  # Пропустим тикеры без данных
                
                # Собирать значения для статистических расчетов
                all_deltas.append(delta)
                all_gammas.append(gamma)
                all_vegas.append(vega)
                all_thetas.append(theta)
                if bid_iv > 0 and ask_iv > 0:
                    all_ivs.append((bid_iv + ask_iv) / 2)
                
                # Delta
                total_delta += delta
                if '-C' in symbol:  # Call option
                    call_delta += delta
                    call_count += 1
                    call_volume += volume
                    call_open_interest += open_interest
                elif '-P' in symbol:  # Put option
                    put_delta += delta
                    put_count += 1
                    put_volume += volume
                    put_open_interest += open_interest
                
                # Gamma
                total_gamma += gamma
                if '-C' in symbol:  # Call option
                    call_gamma += gamma
                elif '-P' in symbol:  # Put option
                    put_gamma += gamma
                
                # Vega
                total_vega += vega
                
                # Theta
                total_theta += theta
                
                # Ро (приблизительно, упрощенный расчет)
                # В реальности требует сложных формул, здесь упрощенная оценка
                rho_approx = vega * 0.01  # Очень упрощенная оценка
                total_rho += rho_approx
                
                # GEX - используем более полную формулу
                # GEX = Gamma × Open Interest × Contract Size × Spot Price² × 0.01
                # Correct contract_size per underlying asset
                contract_size_map = {
                    'BTC': 0.01,
                    'ETH': 0.1,
                    'SOL': 1.0,
                    'USDC': 1.0
                }

                base = symbol.split('-')[0]
                contract_size = contract_size_map.get(base.upper(), 1.0)
                gex_contribution = 0
                dex_contribution = 0
                
                if gamma is not None and open_interest is not None and underlying_price > 0:
                    gex_contribution = gamma * open_interest * contract_size * (underlying_price ** 2) * 0.01
                    gex += gex_contribution
                
                # DEX
                if delta is not None and open_interest is not None:
                    dex_contribution = delta * open_interest
                    dex += dex_contribution
                
                # Store individual contract data
                contract_gex_dex_data.append({
                    'contract_symbol': symbol,
                    'gamma': gamma if gamma is not None else 0,
                    'delta': delta if delta is not None else 0,
                    'open_interest': open_interest if open_interest is not None else 0,
                    'gex': gex_contribution,
                    'dex': dex_contribution
                })
                
                # Ванна (приблизительно)
                # Ванна = dVega/dSpot, упрощенная оценка
                vanna += vega * delta  # Очень упрощенная оценка
                
                # Чарм (приблизительно)
                # Чарм = dDelta/dTime, упрощенная оценка
                charm += delta * theta  # Очень упрощенная оценка
                
                # Vomma (approximately)
                # Vomma = dVega/dIV, simplified estimation
                vomma += vega * (bid_iv + ask_iv) / 2  # Very simplified estimation
                
                # Veta (approximately)
                # Veta = dVega/dTime, simplified estimation
                veta += vega * theta  # Very simplified estimation
                
                # Speed (approximately)
                # Speed = dGamma/dSpot, simplified estimation
                speed += gamma * delta  # Very simplified estimation
                
                # Color (approximately)
                # Color = dGamma/dTime, simplified estimation
                color += gamma * theta  # Very simplified estimation
                
                # Ultima (approximately)
                # Ultima = dVomma/dIV, simplified estimation
                # Correct vol-of-vol accumulation
                mean_iv = (bid_iv + ask_iv) / 2
                local_vomma = vega * mean_iv
                vomma += local_vomma

                # Ultima ≈ d(Vomma)/dIV — apply per-contract
                ultima += local_vomma * mean_iv
                
                # Zomma (approximately)
                # Zomma = dGamma/dIV, simplified estimation
                zomma += gamma * (bid_iv + ask_iv) / 2  # Very simplified estimation
                
                # Option value metrics
                option_premium += last_price
                bid_ask_spread += (ask_price - bid_price) if ask_price > bid_price else 0
                
                # Open Interest and Volume
                open_interest_total += open_interest
                volume_total += volume
                
                # Data for volatility smile
                try:
                    if '-C' in symbol or '-P' in symbol:
                        parts = symbol.split('-')
                        if len(parts) >= 3:
                            # Try to convert penultimate part to number
                            try:
                                strike = float(parts[-2])  # Assuming strike is penultimate part
                                strikes.append(strike)
                                
                                # IV from bid/ask IV
                                iv = (bid_iv + ask_iv) / 2 if bid_iv > 0 and ask_iv > 0 else 0
                                ivs.append(iv)
                                
                                if '-C' in symbol:
                                    call_ivs.append(iv)
                                elif '-P' in symbol:
                                    put_ivs.append(iv)
                            except ValueError:
                                # Failed to convert to number, skip
                                pass
                except Exception as e:
                    # Just skip this record
                    pass
            
            # Если нет валидных тикеров, вернем пустой словарь
            if valid_tickers == 0:
                logger.warning("No valid tickers found for metrics calculation")
                return {}
            
            # Calculate additional option value metrics
            # Даже если underlying_price = 0, попробуем рассчитать остальные метрики
            if len(strikes) > 0:
                # Calculate average intrinsic value
                intrinsic_values = []
                extrinsic_values = []
                moneyness_values = []
                
                for ticker in options_tickers:
                    # avoid silent skipping; log insufficient rows
                    if len(ticker) < 26:
                        logger.warning(f"Incomplete ticker row, expected >=26 columns, got {len(ticker)}")
                        continue
                        
                        
                    symbol = str(ticker[2]) if len(ticker) > 2 and ticker[2] is not None else ""
                    last_price = float(ticker[9]) if len(ticker) > 9 and ticker[9] is not None else 0
                    delta = float(ticker[21]) if len(ticker) > 21 and ticker[21] is not None else 0
                    
                    try:
                        if '-C' in symbol or '-P' in symbol:
                            parts = symbol.split('-')
                            if len(parts) >= 3:
                                strike = float(parts[-2])
                                
                                # Calculate intrinsic value
                                if '-C' in symbol:  # Call option
                                    intrinsic = max(underlying_price - strike, 0) if underlying_price > 0 else 0
                                else:  # Put option
                                    intrinsic = max(strike - underlying_price, 0) if underlying_price > 0 else 0
                                
                                intrinsic_values.append(intrinsic)
                                extrinsic = max(last_price - intrinsic, 0) if last_price >= intrinsic else 0
                                extrinsic_values.append(extrinsic)
                                
                                # Рассчитать монейнесс
                                if strike > 0:
                                    moneyness_val = underlying_price / strike if underlying_price > 0 else 0
                                    moneyness_values.append(moneyness_val)
                    except ValueError:
                        pass
                
                # Average values
                if intrinsic_values:
                    intrinsic_value = sum(intrinsic_values) / len(intrinsic_values)
                if extrinsic_values:
                    extrinsic_value = sum(extrinsic_values) / len(extrinsic_values)
                if moneyness_values:
                    moneyness = sum(moneyness_values) / len(moneyness_values)
            
            # Calculate volatility metrics
            if all_ivs:
                # Implied volatility (average)
                implied_volatility = sum(all_ivs) / len(all_ivs)
                
                # Historical volatility (standard deviation of IV)
                if len(all_ivs) > 1:
                    mean_iv = implied_volatility
                    variance = sum((iv - mean_iv) ** 2 for iv in all_ivs) / (len(all_ivs) - 1)
                    historical_volatility = math.sqrt(variance)
                
                # Realized volatility (calculated from price returns)
                # For a more accurate calculation, we would need historical price data
                # Here we use a simplified approach based on IV changes
                if len(all_ivs) > 1:
                    returns = [all_ivs[i] / all_ivs[i-1] - 1 for i in range(1, len(all_ivs)) if all_ivs[i-1] != 0]
                    if returns:
                        mean_return = sum(returns) / len(returns)
                        variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1) if len(returns) > 1 else 0
                        realized_volatility = math.sqrt(variance)
            
            # Calculate volatility metrics
            if call_ivs and put_ivs:
                avg_call_iv = sum(call_ivs) / len(call_ivs)
                avg_put_iv = sum(put_ivs) / len(put_ivs)
                # Volatility skew (difference between average IV of call and put)
                volatility_skew_metric = avg_call_iv - avg_put_iv
                # Term structure (simplified estimation)
                term_structure = (avg_call_iv + avg_put_iv) / 2
            
            # Рассчитать улыбку волатильности (упрощенно)
            smile_slope = 0
            smile_slope_raw = 0  # Raw slope before any normalization
            smile_curvature = 0
            
            if len(strikes) > 2 and len(ivs) == len(strikes):
                try:
                    # Отсортировать по страйкам
                    sorted_data = sorted(zip(strikes, ivs))
                    strikes_sorted, ivs_sorted = zip(*sorted_data)
                    
                    # Рассчитать наклон (разница между IV на верхнем и нижнем страйках)
                    if len(ivs_sorted) > 2:
                        smile_slope_raw = ivs_sorted[-1] - ivs_sorted[0]  # Raw slope
                        smile_slope = smile_slope_raw  # Keep same for now
                        
                        # Рассчитать кривизну (среднеквадратичное отклонение от линейной аппроксимации)
                        x = list(strikes_sorted)
                        y = list(ivs_sorted)
                        
                        # Линейная аппроксимация (упрощенная реализация)
                        if len(x) > 1:
                            # Рассчитать коэффициенты линейной регрессии
                            n = len(x)
                            sum_x = sum(x)
                            sum_y = sum(y)
                            sum_xy = sum(x[i] * y[i] for i in range(n))
                            sum_x2 = sum(xi * xi for xi in x)
                            
                            # Избежать деления на ноль
                            denom = n * sum_x2 - sum_x * sum_x
                            if denom != 0:
                                slope = (n * sum_xy - sum_x * sum_y) / denom
                                intercept = (sum_y - slope * sum_x) / n
                                
                                # Рассчитать предсказанные значения
                                y_pred = [slope * xi + intercept for xi in x]
                                
                                # Рассчитать среднеквадратичное отклонение
                                mse = sum((y[i] - y_pred[i]) ** 2 for i in range(n)) / n
                                smile_curvature = math.sqrt(mse) if mse >= 0 else 0
                            else:
                                smile_curvature = 0
                        else:
                            smile_curvature = 0
                except Exception as e:
                    logger.warning(f"Error calculating volatility smile: {e}")
            
            # Перекос волатильности (skew)
            volatility_skew = 0
            if call_gamma > 0 and put_gamma > 0:
                volatility_skew = call_gamma - put_gamma
                
            # Put-Call Ratio
            put_call_ratio = put_count / call_count if call_count > 0 else 0
            
            # Calculate distribution statistical metrics
            if all_deltas:
                # Skewness - measure of distribution asymmetry
                n = len(all_deltas)
                if n > 1:
                    mean_delta = sum(all_deltas) / n
                    std_delta = math.sqrt(sum((x - mean_delta) ** 2 for x in all_deltas) / (n - 1)) if n > 1 else 0
                    if std_delta > 0:
                        skewness = (sum((x - mean_delta) ** 3 for x in all_deltas) / n) / (std_delta ** 3)
                    
                    # Kurtosis - measure of distribution "peakedness"
                    if std_delta > 0:
                        kurtosis = (sum((x - mean_delta) ** 4 for x in all_deltas) / n) / (std_delta ** 4) - 3
            
            # Calculate market metrics
            # Market depth (sum of bid and ask volumes)
            total_bid_depth = 0
            total_ask_depth = 0
            for ticker in options_tickers:
                if len(ticker) > 4 and ticker[4] is not None:  # bid1_size
                    total_bid_depth += float(ticker[4]) if ticker[4] else 0
                if len(ticker) > 7 and ticker[7] is not None:  # ask1_size
                    total_ask_depth += float(ticker[7]) if ticker[7] else 0
            market_depth = total_bid_depth + total_ask_depth
            
            # Order imbalance (difference between bid and ask volumes)
            order_imbalance = total_bid_depth - total_ask_depth
            
            # Order flow (approximate estimation based on volume)
            order_flow = volume_total
            
            # Get futures price from options_data if available
            futures_price = 0
            if options_data and len(options_data) > 6:  # index 6 is index_price
                futures_price = float(options_data[6]) if options_data[6] is not None else 0
            
            # Collect all metrics in dictionary
            metrics = {
                # First order Greeks
                'underlying_price': underlying_price,
                'total_delta': total_delta,
                'total_gamma': total_gamma,
                'total_vega': total_vega,
                'total_theta': total_theta,
                'total_rho': total_rho,
                
                # Greeks by option type
                'call_delta': call_delta,
                'put_delta': put_delta,
                'call_gamma': call_gamma,
                'put_gamma': put_gamma,
                
                # Second order Greeks
                'vanna': vanna,
                'charm': charm,
                'vomma': vomma,
                'veta': veta,
                
                # Third order Greeks
                'speed': speed,
                'color': color,
                'ultima': ultima,
                'zomma': zomma,
                
                # Exposures
                'gex': gex,
                'dex': dex,
                
                # Option value metrics
                'intrinsic_value': intrinsic_value,
                'extrinsic_value': extrinsic_value,
                'option_premium': option_premium,
                'moneyness': moneyness,
                'bid_ask_spread': bid_ask_spread,
                
                # Volatility metrics
                'implied_volatility': implied_volatility,
                'historical_volatility': historical_volatility,
                'realized_volatility': realized_volatility,
                'volatility_skew_metric': volatility_skew_metric,
                'term_structure': term_structure,
                'smile_slope': smile_slope,
                'smile_slope_raw': smile_slope_raw,
                'smile_curvature': smile_curvature,
                'volatility_skew': volatility_skew,
                
                # Distribution metrics
                'skewness': skewness,
                'kurtosis': kurtosis,
                
                # Market metrics
                'put_call_ratio': put_call_ratio,
                'open_interest_total': open_interest_total,
                'volume_total': volume_total,
                'market_depth': market_depth,
                'order_imbalance': order_imbalance,
                'order_flow': order_flow,
                'futures_price': futures_price,
                
                # Individual contract data
                'contract_gex_dex_data': contract_gex_dex_data
            }
            
            return metrics
        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return {}

    def create_metrics_database_schema(self, db_path):
        """
        Create database schema for storing metrics
        """
        try:
            # Ensure database folder exists
            db_dir = os.path.dirname(db_path)
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
                
            conn = sqlite3.connect(db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Enable WAL mode for better concurrent access and optimize performance
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA synchronous=NORMAL')  # Faster writes, safer than OFF
            cursor.execute('PRAGMA cache_size=10000')    # Larger cache for better performance
            cursor.execute('PRAGMA foreign_keys=ON')     # Enable foreign keys
            
            # Check if table exists and has correct structure
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_data'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                # Check table structure
                cursor.execute("PRAGMA table_info(metrics_data)")
                columns = cursor.fetchall()
                column_names = [column[1] for column in columns]
                
                # Check if all required columns exist
                required_columns = [
                    'total_rho', 'vanna', 'charm', 'vomma', 'veta', 'speed', 'color', 
                    'ultima', 'zomma', 'intrinsic_value', 'extrinsic_value', 'moneyness',
                    'implied_volatility', 'historical_volatility', 'realized_volatility',
                    'volatility_skew_metric', 'term_structure', 'skewness', 'kurtosis',
                    'market_depth', 'order_imbalance', 'order_flow', 'futures_price', 'smile_slope_raw',
                    'IV_pct', 'RV_pct', 'HV_pct', 'Sell_Score', 'Buy_Score', 'Gap',
                    'w_iv_sell', 'w_rv_sell', 'w_hv_sell', 'w_iv_buy', 'w_rv_buy', 'w_hv_buy'
                ]

                missing_columns = [col for col in required_columns if col not in column_names]
                
                if missing_columns:
                    # If there are missing columns, recreate table
                    # Rename old table
                    cursor.execute("ALTER TABLE metrics_data RENAME TO metrics_data_old")
                    
                    # Create new table with correct structure
                    cursor.execute('''
                        CREATE TABLE metrics_data (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            collection_date TEXT NOT NULL,
                            collection_time TEXT NOT NULL,
                            underlying_price REAL,
                            futures_price REAL,
                            total_delta REAL,
                            total_gamma REAL,
                            total_vega REAL,
                            total_theta REAL,
                            total_rho REAL,
                            call_delta REAL,
                            put_delta REAL,
                            call_gamma REAL,
                            put_gamma REAL,
                            vanna REAL,
                            charm REAL,
                            vomma REAL,
                            veta REAL,
                            speed REAL,
                            color REAL,
                            ultima REAL,
                            zomma REAL,
                            gex REAL,
                            dex REAL,
                            intrinsic_value REAL,
                            extrinsic_value REAL,
                            option_premium REAL,
                            moneyness REAL,
                            bid_ask_spread REAL,
                            implied_volatility REAL,
                            historical_volatility REAL,
                            realized_volatility REAL,
                            volatility_skew_metric REAL,
                            term_structure REAL,
                            smile_slope REAL,
                            smile_curvature REAL,
                            volatility_skew REAL,
                            skewness REAL,
                            kurtosis REAL,
                            put_call_ratio REAL,
                            open_interest_total REAL,
                            volume_total REAL,
                            market_depth REAL,
                            order_imbalance REAL,
                            order_flow REAL,
                            smile_slope_raw REAL,
                            IV_pct REAL,
                            RV_pct REAL,
                            HV_pct REAL,
                            Sell_Score REAL,
                            Buy_Score REAL,
                            Gap REAL,
                            w_iv_sell REAL,
                            w_rv_sell REAL,
                            w_hv_sell REAL,
                            w_iv_buy REAL,
                            w_rv_buy REAL,
                            w_hv_buy REAL
                        )
                    ''')
                    
                    # Copy data from old table (if exists)
                    old_columns = ', '.join([col for col in column_names if col in required_columns + [
                        'id', 'collection_date', 'collection_time', 'underlying_price', 'total_delta', 
                        'total_gamma', 'total_vega', 'total_theta', 'call_delta', 'put_delta', 
                        'call_gamma', 'put_gamma', 'gex', 'dex', 'option_premium', 'bid_ask_spread',
                        'smile_slope', 'smile_curvature', 'volatility_skew', 'put_call_ratio',
                        'open_interest_total', 'volume_total', 'futures_price', 'smile_slope_raw',
                        'IV_pct', 'RV_pct', 'HV_pct', 'Sell_Score', 'Buy_Score', 'Gap',
                        'w_iv_sell', 'w_rv_sell', 'w_hv_sell', 'w_iv_buy', 'w_rv_buy', 'w_hv_buy'
                    ]])
                    
                    if old_columns:
                        try:
                            cursor.execute(f"INSERT INTO metrics_data ({old_columns}) SELECT {old_columns} FROM metrics_data_old")
                        except Exception as e:
                            logger.warning(f"Could not copy data from old table: {e}")
                    
                    # Drop old table
                    cursor.execute("DROP TABLE metrics_data_old")
                # Ensure Options_Greeks has new raw columns if table exists
                cursor.execute("PRAGMA table_info(Options_Greeks)")
                og_cols = [c[1] for c in cursor.fetchall()]
                # columns we may have added later
                new_og_columns = {
                    'markPrice': 'REAL',
                    'markIv': 'REAL',
                    'open_interest': 'REAL',
                    'volume24h': 'REAL',
                    'turnover24h': 'REAL',
                    'bid1_size': 'REAL',
                    'ask1_size': 'REAL',
                    'change24h': 'REAL',
                    'PIN_risk': 'REAL',
                    'bid': 'REAL',
                    'ask': 'REAL',
                    'mid': 'REAL'
                }
                for col, coltype in new_og_columns.items():
                    if col not in og_cols:
                        try:
                            cursor.execute(f"ALTER TABLE Options_Greeks ADD COLUMN {col} {coltype}")
                        except Exception:
                            # ignore if cannot add (older SQLite or other issues)
                            pass
                # If Options_Greeks is missing many columns (older schema), rebuild it
                try:
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Options_Greeks'")
                    if cursor.fetchone():
                        cursor.execute("PRAGMA table_info(Options_Greeks)")
                        existing_cols = [c[1] for c in cursor.fetchall()]
                        expected_og_cols = [
                            'id','collection_id','time','symbol','S','K','sigma','r','q','expiration_date','collection_datetime','T','option_type','theoretical_price','delta','gamma','vega','theta','rho','vanna','markPrice','markIv','volga','charm','veta','speed','zomma','color','ultima','intrinsic_value','time_value','extrinsic_value','premium_ratio','strike_to_spot','distance_to_strike_pct','moneyness','log_moneyness','standardized_moneyness','moneyness_category','prob_ITM','prob_OTM','breakeven','d2_breakeven','prob_profit','expected_value','leverage','lambda_metric','mispricing','bid_ask_spread','bid_ask_spread_pct','mid_price','liquidity_score','volume_oi_ratio','effective_spread','depth_ratio','turnover_ratio','DTE','time_decay_rate','theta_per_day','days_to_breakeven','theta_premium_pct','annualized_return','IV_spread','IV_spread_pct','vega_theta_ratio','delta_gamma_ratio','gamma_vega_ratio','vera','epsilon_call','epsilon_put','PIN_risk','open_interest','volume24h','turnover24h','bid1_size','ask1_size','change24h','bid','ask', 'mid'
                        ]
                        missing_og = [c for c in expected_og_cols if c not in existing_cols]
                        # if substantial columns are missing (beyond small raw additions), rebuild table
                        if len(missing_og) > 5:  # heuristic threshold
                            cursor.execute("ALTER TABLE Options_Greeks RENAME TO Options_Greeks_old")
                            cursor.execute('''
                                CREATE TABLE Options_Greeks (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    collection_id INTEGER,
                                    time TEXT,
                                    symbol TEXT,
                                    S REAL,
                                    K REAL,
                                    sigma REAL,
                                    r REAL,
                                    q REAL,
                                    expiration_date TEXT,
                                    collection_datetime TEXT,
                                    T REAL,
                                    option_type TEXT,
                                    theoretical_price REAL,
                                    delta REAL,
                                    gamma REAL,
                                    vega REAL,
                                    theta REAL,
                                    rho REAL,
                                    vanna REAL,
                                    markPrice REAL,
                                    markIv REAL,
                                    volga REAL,
                                    charm REAL,
                                    veta REAL,
                                    speed REAL,
                                    zomma REAL,
                                    color REAL,
                                    ultima REAL,
                                    intrinsic_value REAL,
                                    time_value REAL,
                                    extrinsic_value REAL,
                                    premium_ratio REAL,
                                    strike_to_spot REAL,
                                    distance_to_strike_pct REAL,
                                    moneyness REAL,
                                    log_moneyness REAL,
                                    standardized_moneyness REAL,
                                    moneyness_category TEXT,
                                    prob_ITM REAL,
                                    prob_OTM REAL,
                                    breakeven REAL,
                                    d2_breakeven REAL,
                                    prob_profit REAL,
                                    expected_value REAL,
                                    leverage REAL,
                                    lambda_metric REAL,
                                    mispricing REAL,
                                    bid_ask_spread REAL,
                                    bid_ask_spread_pct REAL,
                                    mid_price REAL,
                                    liquidity_score REAL,
                                    volume_oi_ratio REAL,
                                    effective_spread REAL,
                                    depth_ratio REAL,
                                    turnover_ratio REAL,
                                    DTE INTEGER,
                                    time_decay_rate REAL,
                                    theta_per_day REAL,
                                    days_to_breakeven REAL,
                                    theta_premium_pct REAL,
                                    annualized_return REAL,
                                    IV_spread REAL,
                                    IV_spread_pct REAL,
                                    vega_theta_ratio REAL,
                                    delta_gamma_ratio REAL,
                                    gamma_vega_ratio REAL,
                                    vera REAL,
                                    epsilon_call REAL,
                                    epsilon_put REAL,
                                    PIN_risk REAL,
                                    /* raw fields for cumulative aggregation */
                                    open_interest REAL,
                                    volume24h REAL,
                                    turnover24h REAL,
                                    bid1_size REAL,
                                    ask1_size REAL,
                                    change24h REAL,
                                    bid REAL,
                                    ask REAL,
                                    mid REAL
                                )
                            ''')
                            # copy overlapping columns
                            overlap = [c for c in existing_cols if c in expected_og_cols]
                            if overlap:
                                cols_csv = ','.join(overlap)
                                try:
                                    cursor.execute(f"INSERT INTO Options_Greeks ({cols_csv}) SELECT {cols_csv} FROM Options_Greeks_old")
                                except Exception as e:
                                    logger.warning(f"Could not copy Options_Greeks old data: {e}")
                            cursor.execute("DROP TABLE Options_Greeks_old")
                except Exception as e:
                    logger.warning(f"Options_Greeks schema check/migration issue: {e}")
            else:
                # Create table for metrics data
                cursor.execute('''
                    CREATE TABLE metrics_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        collection_date TEXT NOT NULL,
                        collection_time TEXT NOT NULL,
                        underlying_price REAL,
                        futures_price REAL,
                        total_delta REAL,
                        total_gamma REAL,
                        total_vega REAL,
                        total_theta REAL,
                        total_rho REAL,
                        call_delta REAL,
                        put_delta REAL,
                        call_gamma REAL,
                        put_gamma REAL,
                        vanna REAL,
                        charm REAL,
                        vomma REAL,
                        veta REAL,
                        speed REAL,
                        color REAL,
                        ultima REAL,
                        zomma REAL,
                        gex REAL,
                        dex REAL,
                        intrinsic_value REAL,
                        extrinsic_value REAL,
                        option_premium REAL,
                        moneyness REAL,
                        bid_ask_spread REAL,
                        implied_volatility REAL,
                        historical_volatility REAL,
                        realized_volatility REAL,
                        volatility_skew_metric REAL,
                        term_structure REAL,
                        smile_slope REAL,
                        smile_curvature REAL,
                        volatility_skew REAL,
                        skewness REAL,
                        kurtosis REAL,
                        put_call_ratio REAL,
                        open_interest_total REAL,
                        volume_total REAL,
                        market_depth REAL,
                        order_imbalance REAL,
                        order_flow REAL,
                        smile_slope_raw REAL,
                        IV_pct REAL,
                        RV_pct REAL,
                        HV_pct REAL,
                        Sell_Score REAL,
                        Buy_Score REAL,
                        Gap REAL,
                        w_iv_sell REAL,
                        w_rv_sell REAL,
                        w_hv_sell REAL,
                        w_iv_buy REAL,
                        w_rv_buy REAL,
                        w_hv_buy REAL
                    )
                ''')
            
            # Create table for individual contract GEX/DEX data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contract_gex_dex (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_date TEXT NOT NULL,
                    collection_time TEXT NOT NULL,
                    contract_symbol TEXT NOT NULL,
                    underlying_price REAL,
                    gamma REAL,
                    delta REAL,
                    open_interest REAL,
                    gex REAL,
                    dex REAL
                )
            ''')

            # Create table for detailed per-contract greeks and metrics (Options_Greeks)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Options_Greeks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_id INTEGER,
                    time TEXT,
                    symbol TEXT,
                    S REAL,
                    K REAL,
                    sigma REAL,
                    r REAL,
                    q REAL,
                    expiration_date TEXT,
                    collection_datetime TEXT,
                    T REAL,
                    option_type TEXT,
                    theoretical_price REAL,
                    delta REAL,
                    gamma REAL,
                    vega REAL,
                    theta REAL,
                    rho REAL,
                    vanna REAL,
                    markPrice REAL,
                    markIv REAL,
                    volga REAL,
                    charm REAL,
                    veta REAL,
                    speed REAL,
                    zomma REAL,
                    color REAL,
                    ultima REAL,
                    intrinsic_value REAL,
                    time_value REAL,
                    extrinsic_value REAL,
                    premium_ratio REAL,
                    strike_to_spot REAL,
                    distance_to_strike_pct REAL,
                    moneyness REAL,
                    log_moneyness REAL,
                    standardized_moneyness REAL,
                    moneyness_category TEXT,
                    prob_ITM REAL,
                    prob_OTM REAL,
                    breakeven REAL,
                    d2_breakeven REAL,
                    prob_profit REAL,
                    expected_value REAL,
                    leverage REAL,
                    lambda_metric REAL,
                    mispricing REAL,
                    bid_ask_spread REAL,
                    bid_ask_spread_pct REAL,
                    mid_price REAL,
                    liquidity_score REAL,
                    volume_oi_ratio REAL,
                    effective_spread REAL,
                    depth_ratio REAL,
                    turnover_ratio REAL,
                    DTE INTEGER,
                    time_decay_rate REAL,
                    theta_per_day REAL,
                    days_to_breakeven REAL,
                    theta_premium_pct REAL,
                    annualized_return REAL,
                    IV_spread REAL,
                    IV_spread_pct REAL,
                    vega_theta_ratio REAL,
                    delta_gamma_ratio REAL,
                    gamma_vega_ratio REAL,
                    vera REAL,
                    epsilon_call REAL,
                    epsilon_put REAL,
                    PIN_risk REAL,
                    /* raw fields for cumulative aggregation */
                    open_interest REAL,
                    volume24h REAL,
                    turnover24h REAL,
                    bid1_size REAL,
                    ask1_size REAL,
                    change24h REAL,
                    bid REAL,
                    ask REAL,
                    mid REAL
                )
            ''')

            # Create table for cumulative/aggregated greeks and metrics
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Options_Greeks_Comulative (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_id INTEGER,
                    time TEXT,
                    base_coin TEXT,
                    n_total INTEGER,
                    n_calls INTEGER,
                    n_puts INTEGER,
                    sum_OI REAL,
                    sum_OI_calls REAL,
                    sum_OI_puts REAL,
                    sum_volume REAL,
                    sum_volume_calls REAL,
                    sum_volume_puts REAL,
                    sum_turnover REAL,
                    sum_turnover_calls REAL,
                    sum_turnover_puts REAL,
                    sum_Delta_OI_calls REAL,
                    sum_Delta_OI_puts REAL,
                    sum_Gamma_OI REAL,
                    sum_Gamma_OI_calls REAL,
                    sum_Gamma_OI_puts REAL,
                    sum_Vega_OI REAL,
                    sum_Vega_OI_calls REAL,
                    sum_Vega_OI_puts REAL,
                    sum_Theta_OI REAL,
                    sum_Theta_OI_calls REAL,
                    sum_Theta_OI_puts REAL,
                    sum_Vanna_OI REAL,
                    sum_Volga_OI REAL,
                    sum_Charm_OI REAL,
                    sum_Veta_OI REAL,
                    sum_Speed_OI REAL,
                    sum_Zomma_OI REAL,
                    sum_Color_OI REAL,
                    sum_Ultima_OI REAL,
                    sum_IV_OI REAL,
                    sum_IV_OI_calls REAL,
                    sum_IV_OI_puts REAL,
                    sum_markPrice_OI REAL,
                    sum_Intrinsic_OI REAL,
                    sum_TimeValue_OI REAL,
                    sum_BidAskSpread_OI REAL,
                    sum_BidAskSpreadPct_OI REAL,
                    sum_bid_size REAL,
                    sum_ask_size REAL,
                    S_current REAL,
                    n_ITM_calls INTEGER,
                    n_ATM_calls INTEGER,
                    n_OTM_calls INTEGER,
                    n_ITM_puts INTEGER,
                    n_ATM_puts INTEGER,
                    n_OTM_puts INTEGER,
                    sum_OI_ITM_calls REAL,
                    sum_OI_OTM_calls REAL,
                    sum_OI_ITM_puts REAL,
                    sum_OI_OTM_puts REAL,
                    sum_OI_DTE_7d REAL,
                    sum_OI_DTE_30d REAL,
                    sum_OI_DTE_90d REAL,
                    sum_OI_DTE_180d REAL,
                    sum_OI_DTE_180plus REAL,
                    sum_DTE_OI REAL,
                    sum_abs_Mispricing_OI REAL,
                    n_active_contracts INTEGER,
                    sum_change24h_OI REAL,
                    sum_VolumeOI_OI REAL,
                    sum_PINrisk_OI REAL,
                    sum_OI_deep_OTM_puts REAL,
                    sum_OI_deep_OTM_calls REAL,
                    total_open_interest REAL,
                    put_call_OI_ratio REAL,
                    total_notional_OI REAL,
                    total_volume_24h REAL,
                    total_call_volume REAL,
                    total_put_volume REAL,
                    put_call_volume_ratio REAL,
                    avg_IV REAL,
                    avg_IV_Calls REAL,
                    avg_IV_Puts REAL,
                    median_IV REAL,
                    IV_Std_Dev REAL,
                    IV_Range REAL,
                    IVR REAL,
                    IVP REAL,
                    IV_Z_Score REAL,
                    IV_52w_min REAL,
                    IV_52w_max REAL,
                    IV_52w_mean REAL,
                    IV_52w_std REAL,
                    IV_data_points INTEGER,
                    max_pain REAL,
                    IV_pct REAL,
                    RV_pct REAL,
                    HV_pct REAL,
                    Sell_Score REAL,
                    Buy_Score REAL,
                    Gap REAL,
                    w_iv_sell REAL,
                    w_rv_sell REAL,
                    w_hv_sell REAL,
                    w_iv_buy REAL,
                    w_rv_buy REAL,
                    w_hv_buy REAL,
                    realized_volatility REAL,
                    historical_volatility REAL
                )
            ''')
            # Ensure Options_Greeks_Comulative has full modern schema (migrate if older)
            try:
                cursor.execute("PRAGMA table_info(Options_Greeks_Comulative)")
                ogc_cols = [c[1] for c in cursor.fetchall()]
                expected_ogc_cols = [
                    'id','collection_id','time','base_coin','n_total','n_calls','n_puts','sum_OI','sum_OI_calls','sum_OI_puts','sum_volume','sum_volume_calls','sum_volume_puts','sum_turnover','sum_turnover_calls','sum_turnover_puts','sum_Delta_OI','sum_Delta_OI_calls','sum_Delta_OI_puts','sum_Gamma_OI','sum_Gamma_OI_calls','sum_Gamma_OI_puts','sum_Vega_OI','sum_Vega_OI_calls','sum_Vega_OI_puts','sum_Theta_OI','sum_Theta_OI_calls','sum_Theta_OI_puts','sum_Vanna_OI','sum_Volga_OI','sum_Charm_OI','sum_Veta_OI','sum_Speed_OI','sum_Zomma_OI','sum_Color_OI','sum_Ultima_OI','sum_IV_OI','sum_IV_OI_calls','sum_IV_OI_puts','sum_markPrice_OI','sum_Intrinsic_OI','sum_TimeValue_OI','sum_BidAskSpread_OI','sum_BidAskSpreadPct_OI','sum_bid_size','sum_ask_size','S_current','n_ITM_calls','n_ATM_calls','n_OTM_calls','n_ITM_puts','n_ATM_puts','n_OTM_puts','sum_OI_ITM_calls','sum_OI_OTM_calls','sum_OI_ITM_puts','sum_OI_OTM_puts','sum_OI_DTE_7d','sum_OI_DTE_30d','sum_OI_DTE_90d','sum_OI_DTE_180d','sum_OI_DTE_180plus','sum_DTE_OI','sum_abs_Mispricing_OI','n_active_contracts','sum_change24h_OI','sum_VolumeOI_OI','sum_PINrisk_OI','sum_OI_deep_OTM_puts','sum_OI_deep_OTM_calls','total_open_interest','put_call_OI_ratio','total_notional_OI','total_volume_24h','total_call_volume','total_put_volume','put_call_volume_ratio','avg_IV','avg_IV_Calls','avg_IV_Puts','median_IV','IV_Std_Dev','IV_Range','IVR','IVP','IV_Z_Score','IV_52w_min','IV_52w_max','IV_52w_mean','IV_52w_std','IV_data_points','max_pain',
                    'IV_pct', 'RV_pct', 'HV_pct', 'Sell_Score', 'Buy_Score', 'Gap',
                    'w_iv_sell', 'w_rv_sell', 'w_hv_sell', 'w_iv_buy', 'w_rv_buy', 'w_hv_buy',
                    'realized_volatility', 'historical_volatility'
                ]
                missing_ogc = [c for c in expected_ogc_cols if c not in ogc_cols]
                # Correct migration logic
                if len(missing_ogc) == 0:
                    pass
                elif 0 < len(missing_ogc) <= 5:
                    # Add missing columns safely
                    logger.info(f"Adding {len(missing_ogc)} missing columns to Options_Greeks_Comulative: {missing_ogc}")
                    for col in missing_ogc:
                        try:
                            if col == 'max_pain' or col.startswith('IV_') or col in ['IVR', 'IVP', 'IV_Z_Score']:
                                cursor.execute(f"ALTER TABLE Options_Greeks_Comulative ADD COLUMN {col} REAL")
                            elif col.endswith('_points'):
                                cursor.execute(f"ALTER TABLE Options_Greeks_Comulative ADD COLUMN {col} INTEGER")
                            elif col.endswith('_count') or col.startswith('n_'):
                                cursor.execute(f"ALTER TABLE Options_Greeks_Comulative ADD COLUMN {col} INTEGER")
                            elif col in ['IV_pct', 'RV_pct', 'HV_pct', 'Sell_Score', 'Buy_Score', 'Gap',
                                       'w_iv_sell', 'w_rv_sell', 'w_hv_sell', 'w_iv_buy', 'w_rv_buy', 'w_hv_buy',
                                       'realized_volatility', 'historical_volatility']:
                                cursor.execute(f"ALTER TABLE Options_Greeks_Comulative ADD COLUMN {col} REAL")
                            else:
                                cursor.execute(f"ALTER TABLE Options_Greeks_Comulative ADD COLUMN {col} REAL")
                        except sqlite3.OperationalError as e:
                            if "duplicate column" not in str(e).lower():
                                logger.warning(f"Column add failed: {col} — {e}")
                    conn.commit()
                    logger.info(f"Successfully added {len(missing_ogc)} columns to Options_Greeks_Comulative")
                else:
                    # FULL REBUILD WHEN SCHEMA DIVERGED
                    logger.warning("Rebuilding Options_Greeks_Comulative due to >5 missing columns")
                    cursor.execute("ALTER TABLE Options_Greeks_Comulative RENAME TO Options_Greeks_Comulative_old")
                    cursor.execute('''
                        CREATE TABLE Options_Greeks_Comulative (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            collection_id INTEGER,
                            time TEXT,
                            base_coin TEXT,
                            n_total INTEGER,
                            n_calls INTEGER,
                            n_puts INTEGER,
                            sum_OI REAL,
                            sum_OI_calls REAL,
                            sum_OI_puts REAL,
                            sum_volume REAL,
                            sum_volume_calls REAL,
                            sum_volume_puts REAL,
                            sum_turnover REAL,
                            sum_turnover_calls REAL,
                            sum_turnover_puts REAL,
                            sum_Delta_OI_calls REAL,
                            sum_Delta_OI_puts REAL,
                            sum_Gamma_OI REAL,
                            sum_Gamma_OI_calls REAL,
                            sum_Gamma_OI_puts REAL,
                            sum_Vega_OI REAL,
                            sum_Vega_OI_calls REAL,
                            sum_Vega_OI_puts REAL,
                            sum_Theta_OI REAL,
                            sum_Theta_OI_calls REAL,
                            sum_Theta_OI_puts REAL,
                            sum_Vanna_OI REAL,
                            sum_Volga_OI REAL,
                            sum_Charm_OI REAL,
                            sum_Veta_OI REAL,
                            sum_Speed_OI REAL,
                            sum_Zomma_OI REAL,
                            sum_Color_OI REAL,
                            sum_Ultima_OI REAL,
                            sum_IV_OI REAL,
                            sum_IV_OI_calls REAL,
                            sum_IV_OI_puts REAL,
                            sum_markPrice_OI REAL,
                            sum_Intrinsic_OI REAL,
                            sum_TimeValue_OI REAL,
                            sum_BidAskSpread_OI REAL,
                            sum_BidAskSpreadPct_OI REAL,
                            sum_bid_size REAL,
                            sum_ask_size REAL,
                            S_current REAL,
                            n_ITM_calls INTEGER,
                            n_ATM_calls INTEGER,
                            n_OTM_calls INTEGER,
                            n_ITM_puts INTEGER,
                            n_ATM_puts INTEGER,
                            n_OTM_puts INTEGER,
                            sum_OI_ITM_calls REAL,
                            sum_OI_OTM_calls REAL,
                            sum_OI_ITM_puts REAL,
                            sum_OI_OTM_puts REAL,
                            sum_OI_DTE_7d REAL,
                            sum_OI_DTE_30d REAL,
                            sum_OI_DTE_90d REAL,
                            sum_OI_DTE_180d REAL,
                            sum_OI_DTE_180plus REAL,
                            sum_DTE_OI REAL,
                            sum_abs_Mispricing_OI REAL,
                            n_active_contracts INTEGER,
                            sum_change24h_OI REAL,
                            sum_VolumeOI_OI REAL,
                            sum_PINrisk_OI REAL,
                            sum_OI_deep_OTM_puts REAL,
                            sum_OI_deep_OTM_calls REAL,
                            total_open_interest REAL,
                            put_call_OI_ratio REAL,
                            total_notional_OI REAL,
                            total_volume_24h REAL,
                            total_call_volume REAL,
                            total_put_volume REAL,
                            put_call_volume_ratio REAL,
                            avg_IV REAL,
                            avg_IV_Calls REAL,
                            avg_IV_Puts REAL,
                            median_IV REAL,
                            IV_Std_Dev REAL,
                            IV_Range REAL,
                            IVR REAL,
                            IVP REAL,
                            IV_Z_Score REAL,
                            IV_52w_min REAL,
                            IV_52w_max REAL,
                            IV_52w_mean REAL,
                            IV_52w_std REAL,
                            IV_data_points INTEGER,
                            max_pain REAL,
                            IV_pct REAL,
                            RV_pct REAL,
                            HV_pct REAL,
                            Sell_Score REAL,
                            Buy_Score REAL,
                            Gap REAL,
                            w_iv_sell REAL,
                            w_rv_sell REAL,
                            w_hv_sell REAL,
                            w_iv_buy REAL,
                            w_rv_buy REAL,
                            w_hv_buy REAL,
                            realized_volatility REAL,
                            historical_volatility REAL
                        )
                    ''')
                    overlap = [c for c in ogc_cols if c in expected_ogc_cols]
                    if overlap:
                        cols_csv = ','.join(overlap)
                        try:
                            cursor.execute(f"INSERT INTO Options_Greeks_Comulative ({cols_csv}) SELECT {cols_csv} FROM Options_Greeks_Comulative_old")
                        except Exception as e:
                            logger.warning(f"Could not copy Options_Greeks_Comulative old data: {e}")
                    cursor.execute("DROP TABLE Options_Greeks_Comulative_old")
            except Exception as e:
                logger.warning(f"Options_Greeks_Comulative schema check/migration issue: {e}")
            
            # Create indexes for frequently queried columns
            try:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Options_Greeks_By_Strike (
                        asset TEXT,
                        date TEXT,
                        strike REAL,
                        call_oi REAL,
                        put_oi REAL,
                        net_oi REAL,
                        call_gex REAL,
                        put_gex REAL,
                        net_gex REAL,
                        call_dex REAL,
                        put_dex REAL,
                        net_dex REAL,
                        PRIMARY KEY (asset, date, strike)
                    )
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Options_Greeks_By_Strike_Cumulative (
                        asset TEXT,
                        date TEXT,
                        strike REAL,
                        call_oi REAL,
                        put_oi REAL,
                        net_oi REAL,
                        call_gex REAL,
                        put_gex REAL,
                        net_gex REAL,
                        call_dex REAL,
                        put_dex REAL,
                        net_dex REAL,
                        PRIMARY KEY (asset, date, strike)
                    )
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Options_Greeks_By_Expiry (
                        asset TEXT,
                        expiry_date TEXT,
                        dte INTEGER,
                        contract_count INTEGER,
                        call_oi REAL,
                        put_oi REAL,
                        net_oi REAL,
                        call_gamma REAL,
                        put_gamma REAL,
                        total_gamma REAL,
                        call_gex REAL,
                        put_gex REAL,
                        net_gex REAL,
                        call_dex REAL,
                        put_dex REAL,
                        net_dex REAL,
                        PRIMARY KEY (asset, expiry_date)
                    )
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Options_Greeks_By_Strike_All_Expiries (
                        asset TEXT,
                        strike REAL,
                        call_oi REAL,
                        put_oi REAL,
                        net_oi REAL,
                        call_gex REAL,
                        put_gex REAL,
                        net_gex REAL,
                        call_dex REAL,
                        put_dex REAL,
                        net_dex REAL,
                        PRIMARY KEY (asset, strike)
                    )
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS Options_Volatility (
                        asset TEXT,
                        expiry TEXT,
                        strike REAL,
                        type TEXT,
                        iv REAL,
                        delta REAL,
                        gamma REAL,
                        vega REAL,
                        bid REAL,
                        ask REAL,
                        mid REAL,
                        PRIMARY KEY (asset, expiry, strike, type)
                    )
                ''')

                cursor.execute('CREATE INDEX IF NOT EXISTS idx_Options_Greeks_collection_id ON Options_Greeks(collection_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_Options_Greeks_base_coin ON Options_Greeks(base_coin)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_Options_Greeks_time ON Options_Greeks(time)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_Options_Details_collection_id ON Options_Details(collection_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_Options_Tickers_collection_id ON Options_Tickers(collection_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_Options_Greeks_Comulative_base_coin_time ON Options_Greeks_Comulative(base_coin, time)')
                logger.info("Database indexes created successfully")
            except Exception as e:
                logger.warning(f"Could not create indexes: {e}")
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error creating/updating metrics database schema at {db_path}: {e}")

    def percentile_of_score(self, hist_arr, x):
        """
        Calculate percentile rank of x in hist_arr.
        Returns value in [0, 1].
        """
        n = len(hist_arr)
        if n == 0:
            return float('nan')
        less = sum(1 for val in hist_arr if val < x)
        equal = sum(1 for val in hist_arr if val == x)
        return (less + 0.5 * equal) / n

    def rolling_percentile(self, series_values, window):
        """
        Calculate rolling percentile for a series of values.
        Returns array of percentiles with nan for insufficient history.
        """
        n = len(series_values)
        result = [float('nan')] * n
        
        for i in range(window-1, n):
            hist = series_values[i-window+1 : i+1]
            cur = series_values[i]
            result[i] = self.percentile_of_score(hist, cur)
            
        return result

    def calculate_enhanced_ivr_scores(self, symbol, current_iv, current_rv, current_hv, metrics_db):
        """
        Calculate enhanced IVR scores with percentile normalization and adaptive weights.
        
        Args:
            symbol: Asset symbol (BTC, ETH, SOL)
            current_iv: Current implied volatility
            current_rv: Current realized volatility
            current_hv: Current historical volatility
            metrics_db: Path to metrics database
            
        Returns:
            dict: Enhanced IVR metrics or None
        """
        try:
            # Check cache first
            cache_key = (symbol, round(current_iv, 2) if current_iv else None, 
                        round(current_rv, 2) if current_rv else None,
                        round(current_hv, 2) if current_hv else None)
            if cache_key in self._enhanced_ivr_cache:
                return self._enhanced_ivr_cache[cache_key]
            
            if current_iv is None or current_iv <= 0:
                return None
                
            conn = sqlite3.connect(metrics_db, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get historical data for IV, RV, and HV over rolling window (252 trading days)
            query = """
                SELECT avg_IV, realized_volatility, historical_volatility, time
                FROM Options_Greeks_Comulative 
                WHERE base_coin = ? 
                  AND avg_IV IS NOT NULL
                  AND avg_IV > 0
                  AND realized_volatility IS NOT NULL
                  AND historical_volatility IS NOT NULL
                ORDER BY time DESC
                LIMIT 252
            """
            cursor.execute(query, (symbol,))
            rows = cursor.fetchall()
            
            # If insufficient data, try to get all available data
            if not rows or len(rows) < 10:  # Minimum 10 points for statistics
                query_all = """
                    SELECT avg_IV, realized_volatility, historical_volatility, time
                    FROM Options_Greeks_Comulative 
                    WHERE base_coin = ? 
                      AND avg_IV IS NOT NULL
                      AND avg_IV > 0
                      AND realized_volatility IS NOT NULL
                      AND historical_volatility IS NOT NULL
                    ORDER BY time DESC
                    LIMIT 252
                """
                cursor.execute(query_all, (symbol,))
                rows = cursor.fetchall()
            
            conn.close()
            
            if not rows or len(rows) < 2:
                logger.debug(f"Insufficient historical data for enhanced IVR calculation for {symbol}")
                # Return default values when insufficient data
                return {
                    'IV_pct': 0.5,
                    'RV_pct': 0.5,
                    'HV_pct': 0.5,
                    'Sell_Score': 0.5,
                    'Buy_Score': 0.5,
                    'Gap': 0.0,
                    'w_iv_sell': 0.33,
                    'w_rv_sell': 0.33,
                    'w_hv_sell': 0.34,
                    'w_iv_buy': 0.33,
                    'w_rv_buy': 0.33,
                    'w_hv_buy': 0.34
                }
            
            # Extract historical series and normalize to percentages if needed (handle mixed data)
            iv_history = [row[0] * 100 if row[0] is not None and row[0] < 5 else row[0] for row in rows if row[0] is not None]
            rv_history = [row[1] * 100 if row[1] is not None and row[1] < 5 else row[1] for row in rows if row[1] is not None]
            hv_history = [row[2] * 100 if row[2] is not None and row[2] < 5 else row[2] for row in rows if row[2] is not None]
            
            if len(iv_history) < 2 or len(rv_history) < 2 or len(hv_history) < 2:
                logger.debug(f"Insufficient valid historical data for enhanced IVR calculation for {symbol}")
                # Return default values when insufficient data
                return {
                    'IV_pct': 0.5,
                    'RV_pct': 0.5,
                    'HV_pct': 0.5,
                    'Sell_Score': 0.5,
                    'Buy_Score': 0.5,
                    'Gap': 0.0,
                    'w_iv_sell': 0.33,
                    'w_rv_sell': 0.33,
                    'w_hv_sell': 0.34,
                    'w_iv_buy': 0.33,
                    'w_rv_buy': 0.33,
                    'w_hv_buy': 0.34
                }
            
            # Calculate percentiles using rolling window approach
            window = min(252, len(iv_history))  # Use available data if less than 252 days
            
            # Calculate current percentiles
            iv_pct = self.percentile_of_score(iv_history, current_iv)
            rv_pct = self.percentile_of_score(rv_history, current_rv) if current_rv is not None else 0.5
            hv_pct = self.percentile_of_score(hv_history, current_hv) if current_hv is not None else 0.5
            
            # Calculate gap (IV relative to HV) with improved formula
            # Inputs current_iv and current_hv are already in percentages
            eps = 1e-9
            if current_hv is None or current_hv < 1e-6:
                gap = 0.0
            else:
                # Gap = (IV% - HV%) / HV% * 100 to get percentage difference
                gap = ((current_iv - current_hv) / current_hv) * 100
            
            # Enhanced adaptive weights for sell score
            # When IV is significantly higher than HV, give more weight to IV
            # Use sigmoid function for smoother transitions
            w_iv_sell = 0.2 + 0.6 * (1 / (1 + math.exp(-5 * gap)))  # Sigmoid scaling
            w_iv_sell = max(0.1, min(0.8, w_iv_sell))  # Clamp between 0.1 and 0.8
            
            # Distribute remaining weight between RV and HV based on their percentiles
            remaining_weight = 1.0 - w_iv_sell
            denom = (rv_pct + hv_pct + eps)
            
            # Adjust weights based on volatility levels
            # When both RV and HV are high, give more weight to HV
            # When both are low, give more weight to RV
            hv_preference_factor = min(1.0, current_hv / (sum(hv_history) / len(hv_history) + eps))
            rv_weight_adjustment = 0.8 + 0.2 * (1 - hv_preference_factor)
            hv_weight_adjustment = 0.8 + 0.2 * hv_preference_factor
            
            w_rv_sell = remaining_weight * (rv_pct / denom) * rv_weight_adjustment
            w_hv_sell = remaining_weight * (hv_pct / denom) * hv_weight_adjustment
            
            # Normalize weights to sum to remaining_weight
            weight_sum = w_rv_sell + w_hv_sell
            if weight_sum > 0:
                w_rv_sell = w_rv_sell * remaining_weight / weight_sum
                w_hv_sell = w_hv_sell * remaining_weight / weight_sum
            
            # Calculate sell score with additional factors
            sell_score = w_iv_sell * iv_pct + w_rv_sell * rv_pct + w_hv_sell * hv_pct
            
            # Enhanced adaptive weights for buy score
            # When IV is significantly lower than HV, give more weight to HV/RV
            gap_buy = -gap  # Invert the gap for buy side
            w_iv_buy = 0.1 + 0.5 * (1 / (1 + math.exp(-5 * gap_buy)))  # Sigmoid scaling
            w_iv_buy = max(0.05, min(0.6, w_iv_buy))  # Clamp between 0.05 and 0.6
            
            # Distribute remaining weight between HV and RV based on their percentiles
            remaining_weight_buy = 1.0 - w_iv_buy
            
            # For buying, we prefer lower volatility metrics
            # So we use inverse percentiles (1 - percentile)
            inv_rv_pct = 1 - rv_pct
            inv_hv_pct = 1 - hv_pct
            inv_denom = (inv_rv_pct + inv_hv_pct + eps)
            
            # Adjust weights for buying strategy
            # When HV is low, give more weight to HV (better buying opportunity)
            # When RV is low, give more weight to RV (less risk)
            hv_buy_factor = max(0.1, 1 - (current_hv / (sum(hv_history) / len(hv_history) + eps)))
            rv_buy_factor = max(0.1, 1 - (current_rv / (sum(rv_history) / len(rv_history) + eps)))
            
            w_hv_buy = remaining_weight_buy * (inv_hv_pct / inv_denom) * hv_buy_factor
            w_rv_buy = remaining_weight_buy * (inv_rv_pct / inv_denom) * rv_buy_factor
            
            # Normalize weights to sum to remaining_weight_buy
            weight_sum_buy = w_hv_buy + w_rv_buy
            if weight_sum_buy > 0:
                w_hv_buy = w_hv_buy * remaining_weight_buy / weight_sum_buy
                w_rv_buy = w_rv_buy * remaining_weight_buy / weight_sum_buy
            
            # Calculate buy score (using inverse percentiles)
            buy_score = w_iv_buy * (1 - iv_pct) + w_rv_buy * (1 - rv_pct) + w_hv_buy * (1 - hv_pct)
            
            logger.debug(f"Enhanced IVR metrics for {symbol}: Sell_Score={sell_score:.3f}, Buy_Score={buy_score:.3f}")
            
            return {
                'IV_pct': iv_pct,
                'RV_pct': rv_pct,
                'HV_pct': hv_pct,
                'Sell_Score': sell_score,
                'Buy_Score': buy_score,
                'Gap': gap,
                'w_iv_sell': w_iv_sell,
                'w_rv_sell': w_rv_sell,
                'w_hv_sell': w_hv_sell,
                'w_iv_buy': w_iv_buy,
                'w_rv_buy': w_rv_buy,
                'w_hv_buy': w_hv_buy
            }
            
            # Cache result
            self._enhanced_ivr_cache[cache_key] = result
            return result
            
        except Exception as e:
            logger.error(f"Error calculating enhanced IVR for {symbol}: {e}")
            return None

    def calculate_IVR(self, symbol, current_avg_IV, metrics_db):
        """
        Рассчитать IVR (Implied Volatility Rank) для символа на основе исторических данных avg_IV.
        IVR показывает, где текущая IV находится относительно диапазона за последний год.
        
        Args:
            symbol: Тикер актива (BTC, ETH, SOL)
            current_avg_IV: Текущее значение средней IV
            metrics_db: Путь к базе данных метрик
            
        Returns:
            dict: {'IVR': float, 'IVP': float, 'IV_Z_Score': float} или None
        """
        try:
            # Check cache first
            cache_key = (symbol, round(current_avg_IV, 2))
            if cache_key in self._ivr_cache:
                return self._ivr_cache[cache_key]
            
            if current_avg_IV is None or current_avg_IV <= 0:
                return None
                
            conn = sqlite3.connect(metrics_db, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Получить avg_IV за последний год (365 дней) или максимум доступных данных
            query = """
                SELECT avg_IV, time
                FROM Options_Greeks_Comulative 
                WHERE base_coin = ? 
                  AND avg_IV IS NOT NULL
                  AND avg_IV > 0
                ORDER BY time DESC
                LIMIT 365
            """
            cursor.execute(query, (symbol,))
            rows = cursor.fetchall()
            
            # Если данных нет, попробуем получить все доступные данные
            if not rows or len(rows) < 10:  # Минимум 10 точек для статистики
                query_all = """
                    SELECT avg_IV, time
                    FROM Options_Greeks_Comulative 
                    WHERE base_coin = ? 
                      AND avg_IV IS NOT NULL
                      AND avg_IV > 0
                    ORDER BY time DESC
                    LIMIT 365
                """
                cursor.execute(query_all, (symbol,))
                rows = cursor.fetchall()
            
            conn.close()
            
            if not rows or len(rows) < 2:
                logger.debug(f"Insufficient historical data for IVR calculation for {symbol}")
                return None
            
            iv_history = [row[0] for row in rows]
            
            # Рассчитать IVR с защитой от выбросов (используем перцентили)
            # Для более стабильного расчета используем 5-й и 95-й перцентили
            iv_sorted = sorted(iv_history)
            n = len(iv_sorted)
            
            # Стандартный расчет с min/max
            min_IV = min(iv_sorted)
            max_IV = max(iv_sorted)
            
            # Перцентильный расчет (более устойчивый к выбросам)
            percentile_5_idx = max(0, int(n * 0.05))
            percentile_95_idx = min(n-1, int(n * 0.95))
            percentile_5 = iv_sorted[percentile_5_idx]
            percentile_95 = iv_sorted[percentile_95_idx]
            
            # IVR (Implied Volatility Rank) - стандартный расчет
            denominator = max_IV - min_IV
            if denominator < 1e-6:
                IVR = 50.0
            else:
                IVR = ((current_avg_IV - min_IV) / denominator) * 100.0
            
            # IVP (Implied Volatility Percentile) - более устойчивая метрика
            # Показывает процент дней, когда IV была ниже текущей
            days_below = sum(1 for iv in iv_history if iv < current_avg_IV)
            IVP = (days_below / len(iv_history)) * 100
            
            # IV Z-Score - показывает отклонение от среднего в единицах стандартного отклонения
            mean_IV = sum(iv_history) / len(iv_history)
            variance = sum((iv - mean_IV) ** 2 for iv in iv_history) / len(iv_history)
            std_IV = math.sqrt(variance)
            
            if std_IV == 0 or std_IV < 0.0001:
                IV_Z_Score = 0.0
            else:
                IV_Z_Score = (current_avg_IV - mean_IV) / std_IV
            
            logger.debug(f"IVR metrics for {symbol}: IVR={IVR:.2f}%, IVP={IVP:.2f}%, Z-Score={IV_Z_Score:.2f} (n={n})")
            
            result = {
                'IVR': IVR,
                'IVP': IVP,
                'IV_Z_Score': IV_Z_Score,
                'IV_52w_min': min_IV,
                'IV_52w_max': max_IV,
                'IV_52w_mean': mean_IV,
                'IV_52w_std': std_IV,
                'IV_data_points': n
            }
            
            # Cache result
            self._ivr_cache[cache_key] = result
            return result
            
        except Exception as e:
            logger.error(f"Error calculating IVR for {symbol}: {e}")
            return None

    def calculate_max_pain(self, symbol, collection_id, metrics_db):
        """
        Рассчитать Max Pain - цену страйка, при которой держатели опционов понесут максимальные убытки.
        
        Max Pain основан на теории, что цена базового актива стремится к точке, где:
        - Максимальное количество опционов истекает без прибыли
        - Продавцы опционов (маркет-мейкеры) несут минимальные убытки
        
        Формула:
        Для каждого возможного страйка K рассчитываем суммарную стоимость всех опционов:
        Total_Value(K) = Σ[Call_OI(Ki) × max(0, K - Ki)] + Σ[Put_OI(Kj) × max(0, Kj - K)]
        
        Max Pain = Страйк K, при котором Total_Value(K) минимальна
        
        Args:
            symbol: Тикер актива (BTC, ETH, SOL)
            collection_id: ID коллекции для расчета
            metrics_db: Путь к базе данных метрик
            
        Returns:
            float: Цена Max Pain или None
        """
        try:
            conn = sqlite3.connect(metrics_db, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Получить все опционы с их страйками, типами и OI для данной коллекции
            query = """
                SELECT K, option_type, open_interest
                FROM Options_Greeks
                WHERE collection_id = ?
                  AND K IS NOT NULL
                  AND K > 0
                  AND open_interest IS NOT NULL
                  AND open_interest > 0
                ORDER BY K
            """
            cursor.execute(query, (collection_id,))
            rows = cursor.fetchall()
            conn.close()
            
            if not rows or len(rows) < 2:
                logger.debug(f"Insufficient options data for Max Pain calculation for {symbol}")
                return None
            
            # Собрать уникальные страйки и сгруппировать данные
            strike_data = {}  # {strike: {'call_oi': float, 'put_oi': float}}
            
            for row in rows:
                strike = float(row[0])
                opt_type = row[1]
                oi = float(row[2])
                
                if strike not in strike_data:
                    strike_data[strike] = {'call_oi': 0.0, 'put_oi': 0.0}
                
                if opt_type == 'C':
                    strike_data[strike]['call_oi'] += oi
                elif opt_type == 'P':
                    strike_data[strike]['put_oi'] += oi
            
            # Получить список всех уникальных страйков
            strikes = sorted(strike_data.keys())
            
            if len(strikes) < 2:
                logger.debug(f"Not enough unique strikes for Max Pain calculation for {symbol}")
                return None
            
            # Рассчитать Total Value для каждого возможного страйка
            min_total_value = float('inf')
            max_pain_strike = None
            
            for test_strike in strikes:
                total_value = 0.0
                
                # Для каждого страйка рассчитать убытки при текущей тестовой цене
                for strike, data in strike_data.items():
                    # Call опционы: убыток = OI × max(0, test_strike - strike)
                    # Call ITM когда test_strike > strike
                    if test_strike > strike:
                        call_loss = data['call_oi'] * (test_strike - strike)
                        total_value += call_loss
                    
                    # Put опционы: убыток = OI × max(0, strike - test_strike)
                    # Put ITM когда strike > test_strike
                    if strike > test_strike:
                        put_loss = data['put_oi'] * (strike - test_strike)
                        total_value += put_loss
                
                # Найти страйк с минимальной суммарной стоимостью
                if total_value < min_total_value:
                    min_total_value = total_value
                    max_pain_strike = test_strike
            
            if max_pain_strike is not None:
                logger.debug(f"Max Pain for {symbol}: ${max_pain_strike:.2f} (Total Value: ${min_total_value:.2f})")
            
            return max_pain_strike
            
        except Exception as e:
            logger.error(f"Error calculating Max Pain for {symbol}: {e}")
            return None

    def save_contract_gex_dex_to_db(self, symbol, contract_data, collection_date, collection_time):
        """
        Сохранить GEX и DEX для каждого опционного контракта в базу данных
        """
        max_retries = 3
        retry_delay = 0.5  # seconds
        
        for attempt in range(max_retries):
            try:
                # Получить путь к базе данных метрик для этого символа
                symbol_folder = os.path.join(self.metrics_folder, f"Bybit_{symbol}")
                db_path = os.path.join(symbol_folder, f"Bybit_{symbol}_Op_Metrics.db")
                
                # Подключиться к базе данных с увеличенным timeout
                conn = sqlite3.connect(db_path, timeout=30.0)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Подготовить данные для batch insert
                values_list = []
                for contract in contract_data:
                    values = (
                        collection_date,
                        collection_time,
                        contract['contract_symbol'],
                        contract.get('underlying_price', 0),
                        contract.get('gamma', 0),
                        contract.get('delta', 0),
                        contract.get('open_interest', 0),
                        contract.get('gex', 0),
                        contract.get('dex', 0)
                    )
                    values_list.append(values)
                
                # Batch insert instead of individual inserts
                insert_query = '''
                    INSERT INTO contract_gex_dex (
                        collection_date,
                        collection_time,
                        contract_symbol,
                        underlying_price,
                        gamma,
                        delta,
                        open_interest,
                        gex,
                        dex
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                
                cursor.executemany(insert_query, values_list)
                
                conn.commit()
                conn.close()
                
                logger.info(f"Saved {len(contract_data)} contract GEX/DEX records for {symbol}")
                return True
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"Database locked for {symbol}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay *= 1.5  # exponential backoff
                else:
                    logger.error(f"Error saving contract GEX/DEX data to database for {symbol}: {e}")
                    return False
            except Exception as e:
                logger.error(f"Error saving contract GEX/DEX data to database for {symbol}: {e}")
                return False

    # --- Helpers and Options_Greeks calculations ---
    def _norm_cdf(self, x):
        """Standard normal CDF"""
        try:
            x = 0.0 if x is None else float(x)
            return float(0.5 * (1.0 + math.erf(x / math.sqrt(2.0))))
        except Exception as e:
            import logging
            logging.error(f"_norm_cdf error: x={x}, type={type(x)}, err={e}")
            return 0.0

    def _norm_pdf(self, x):
        """Standard normal PDF"""
        try:
            x = 0.0 if x is None else float(x)
            return float(math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi))
        except Exception as e:
            import logging
            logging.error(f"_norm_pdf error: x={x}, type={type(x)}, err={e}")
            return 0.0

    def _parse_symbol(self, symbol):
        """Attempt to extract strike, expiration date and option type from symbol string.
        Returns (K: float or None, expiration_date: datetime.date or None, opt_type: 'C'/'P' or None)
        """
        import re
        K = None
        exp_date = None
        opt_type = None

        if symbol is None:
            return K, exp_date, opt_type

        # Type
        if '-C' in symbol or symbol.endswith('C'):
            opt_type = 'C'
        elif '-P' in symbol or symbol.endswith('P'):
            opt_type = 'P'

        # Strike: look for a numeric group (with optional decimal)
        m = re.findall(r"\d+\.?\d*", symbol)
        if m:
            try:
                # Prefer a numeric token that looks like a strike (4+ digits or >1000)
                strike_token = None
                for tok in reversed(m):
                    if len(tok) >= 4 or int(tok) > 1000:
                        strike_token = tok
                        break
                if strike_token is None:
                    # fallback: choose the last numeric token
                    strike_token = m[-1]
                K = float(strike_token)
            except Exception:
                K = None

        # Expiration: look for common date formats in symbol
        # 1) YYYY-MM-DD or YYYYMMDD
        m2 = re.search(r"(\d{4}-\d{2}-\d{2})|(\d{8})", symbol)
        if m2:
            datestr = m2.group(0)
            try:
                if '-' in datestr:
                    exp_date = datetime.datetime.strptime(datestr, '%Y-%m-%d').date()
                else:
                    exp_date = datetime.datetime.strptime(datestr, '%Y%m%d').date()
            except Exception:
                exp_date = None

        # 2) Formats like 26DEC25 or 25SEP26 (DDMONYY or YYMONDD variants)
        if exp_date is None:
            m3 = re.search(r"(\d{1,2}[A-Za-z]{3}\d{2,4})", symbol)
            if m3:
                token = m3.group(0)
                # Try to parse DDMONYY or DDMONYYYY
                try:
                    # Normalize token to DDMONYYYY
                    day = int(re.match(r"^(\d{1,2})", token).group(1))
                    mon = re.search(r"[A-Za-z]{3}", token).group(0)
                    yr = re.search(r"(\d{2,4})$", token).group(0)
                    month_map = {
                        'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
                        'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12
                    }
                    mon_u = mon.upper()
                    if mon_u in month_map:
                        month = month_map[mon_u]
                        y = int(yr)
                        if y < 100:
                            # assume 2000-2099 for two-digit years
                            y += 2000
                        exp_date = datetime.date(y, month, day)
                except Exception:
                    exp_date = None

        return K, exp_date, opt_type

    def calculate_option_greeks_for_ticker(self, ticker, collection_datetime=None, r=0.02, q=0, options_details=None):
        import logging
        # Safe arithmetic helpers
        def safe_float(val):
            try:
                if val is None:
                    return 0.0
                return float(val)
            except Exception:
                return 0.0
        def safe_div(a, b):
            try:
                a = safe_float(a)
                b = safe_float(b)
                return a / b if b != 0 else 0.0
            except Exception:
                return 0.0
        def safe_mul(*args):
            result = 1.0
            for arg in args:
                result *= safe_float(arg)
            return result
        def safe_add(*args):
            return sum(safe_float(arg) for arg in args)
        """Calculate greeks and metrics for a single ticker tuple according to the provided formulas.
        Returns a dict ready to be saved into Options_Greeks table.
        """
        try:
            # Defensive field extraction with safe float conversion
            def safe_float(val):
                try:
                    if val is None:
                        return 0.0
                    return float(val)
                except Exception:
                    return 0.0

            symbol = str(ticker[2]) if len(ticker) > 2 and ticker[2] is not None else ''
            bid_price = safe_float(ticker[3]) if len(ticker) > 3 else 0.0
            bid_size = safe_float(ticker[4]) if len(ticker) > 4 else 0.0
            bid_iv = safe_float(ticker[5]) if len(ticker) > 5 else 0.0
            ask_price = safe_float(ticker[6]) if len(ticker) > 6 else 0.0
            ask_size = safe_float(ticker[7]) if len(ticker) > 7 else 0.0
            ask_iv = safe_float(ticker[8]) if len(ticker) > 8 else 0.0
            markPrice = safe_float(ticker[12]) if len(ticker) > 12 else 0.0
            underlying = safe_float(ticker[15]) if len(ticker) > 15 else 0.0
            mark_iv_field = safe_float(ticker[14]) if len(ticker) > 14 else 0.0
            open_interest = safe_float(ticker[16]) if len(ticker) > 16 else 0.0
            volume24h = safe_float(ticker[18]) if len(ticker) > 18 else 0.0
            delta = safe_float(ticker[21]) if len(ticker) > 21 else 0.0
            gamma = safe_float(ticker[22]) if len(ticker) > 22 else 0.0
            vega = safe_float(ticker[23]) if len(ticker) > 23 else 0.0
            theta = safe_float(ticker[24]) if len(ticker) > 24 else 0.0
            # Log all extracted variables for debugging
            logging.error(f"Vars: symbol={symbol}, bid_price={bid_price}, bid_size={bid_size}, bid_iv={bid_iv}, ask_price={ask_price}, ask_size={ask_size}, ask_iv={ask_iv}, markPrice={markPrice}, underlying={underlying}, mark_iv_field={mark_iv_field}, open_interest={open_interest}, volume24h={volume24h}, delta={delta}, gamma={gamma}, vega={vega}, theta={theta}")

            # markIv: prefer explicit mark_iv field from ticker, then fall back to average of bid_iv and ask_iv
            markIv = mark_iv_field if mark_iv_field and mark_iv_field > 0 else 0.0
            if (not markIv) and bid_iv > 0 and ask_iv > 0:
                markIv = safe_div(safe_add(bid_iv, ask_iv), 2)
            elif (not markIv) and bid_iv > 0:
                markIv = bid_iv
            elif (not markIv) and ask_iv > 0:
                markIv = ask_iv

            K, exp_date, opt_type = self._parse_symbol(symbol)

            # If expiration not found in symbol, try options_details (delivery_time) when available
            if exp_date is None and options_details:
                try:
                    for det in options_details:
                        # options_details table: symbol is index 2, delivery_time index 9 (see data_collector)
                        det_symbol = det[2] if len(det) > 2 else None
                        det_delivery = det[9] if len(det) > 9 else None
                        if det_symbol and det_symbol == symbol and det_delivery:
                            # delivery_time may be an epoch (ms) string or ISO-like string
                            try:
                                s = str(det_delivery)
                                # epoch in milliseconds
                                if s.isdigit():
                                    ts = int(s)
                                    if ts > 1e12:  # likely milliseconds
                                        ts = ts / 1000.0
                                    dt = datetime.datetime.utcfromtimestamp(ts)
                                    exp_date = dt.date()
                                    break
                                # ISO with trailing Z (e.g. 2025-12-26T00:00:00Z)
                                s2 = s.replace('Z', '')
                                try:
                                    dt = datetime.datetime.fromisoformat(s2)
                                    exp_date = dt.date()
                                    break
                                except Exception:
                                    # try common formats
                                    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                                        try:
                                            dt = datetime.datetime.strptime(s2, fmt)
                                            exp_date = dt.date()
                                            break
                                        except Exception:
                                            continue
                                    if exp_date is not None:
                                        break
                            except Exception:
                                continue
                except Exception:
                    pass

            S = underlying if underlying > 0 else (markPrice if markPrice > 0 else 0.0)

            # collection datetime string
            coll_dt_str = ''
            if collection_datetime is not None:
                if isinstance(collection_datetime, datetime.datetime):
                    coll_dt_str = collection_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    coll_date_only = collection_datetime.date()
                else:
                    try:
                        coll_dt = datetime.datetime.strptime(collection_datetime, '%Y-%m-%d %H:%M:%S')
                        coll_dt_str = collection_datetime
                        coll_date_only = coll_dt.date()
                    except Exception:
                        coll_date_only = None
            else:
                coll_date_only = None

            # compute T (in years)
            T = None
            DTE = None
            if exp_date is not None and coll_date_only is not None:
                delta_days = (exp_date - coll_date_only).days
                DTE = delta_days
                T = max(delta_days / 365.0, 0.0)
            else:
                T = 0.0

            sigma = safe_float(markIv)
            sqrt_T = math.sqrt(T) if T and T > 0 else 0.0
            sigma_sqrt_T = safe_mul(sigma, sqrt_T)
            ln_S_K = 0.0
            if K and S and K > 0 and S > 0:
                try:
                    ln_S_K = math.log(safe_div(S, K))
                except Exception:
                    ln_S_K = 0.0
            sigma_squared = safe_mul(sigma, sigma)
            sigma_squared_half = safe_div(sigma_squared, 2.0) if sigma_squared is not None else 0.0
            exp_minus_rT = math.exp(-safe_mul(r, T)) if T is not None else 1.0
            exp_minus_qT = 1.0
            S_squared = safe_mul(S, S)
            if sigma_sqrt_T > 0:
                d1 = safe_div(ln_S_K + safe_mul(safe_add(r, sigma_squared_half), T), sigma_sqrt_T)
                d2 = d1 - sigma_sqrt_T
            else:
                d1 = 0.0
                d2 = 0.0

            N_d1 = safe_float(self._norm_cdf(safe_float(d1)))
            N_d2 = safe_float(self._norm_cdf(safe_float(d2)))
            N_minus_d1 = safe_float(self._norm_cdf(-safe_float(d1)))
            N_minus_d2 = safe_float(self._norm_cdf(-safe_float(d2)))
            n_d1 = safe_float(self._norm_pdf(safe_float(d1)))
            import logging
            logging.error(f"Greeks operands: d1={d1} (type {type(d1)}), d2={d2} (type {type(d2)}), N_d1={N_d1} (type {type(N_d1)}), N_d2={N_d2} (type {type(N_d2)}), N_minus_d1={N_minus_d1} (type {type(N_minus_d1)}), N_minus_d2={N_minus_d2} (type {type(N_minus_d2)}), n_d1={n_d1} (type {type(n_d1)})")

            # Theoretical price
            theoretical_price = 0.0
            if opt_type == 'C':
                theoretical_price = safe_add(safe_mul(S, N_d1), -safe_mul(K, exp_minus_rT, N_d2)) if K and S > 0 else 0.0
            elif opt_type == 'P':
                theoretical_price = safe_add(safe_mul(K, exp_minus_rT, N_minus_d2), -safe_mul(S, N_minus_d1)) if K and S > 0 else 0.0
            else:
                theoretical_price = safe_float(markPrice)

            # First order greeks
            Delta = N_d1 if opt_type == 'C' else (N_d1 - 1)
            Gamma = safe_div(n_d1, safe_mul(S, sigma_sqrt_T)) if (S > 0 and sigma_sqrt_T > 0) else 0.0
            Vega = safe_mul(S, n_d1, sqrt_T) if sqrt_T > 0 else 0.0
            Theta = 0.0
            if sqrt_T > 0:
                if opt_type == 'C':
                    Theta = safe_add(-safe_mul(S, n_d1, sigma) / (2 * safe_float(sqrt_T)), -safe_mul(r, K, exp_minus_rT, N_d2))
                else:
                    Theta = safe_add(-safe_mul(S, n_d1, sigma) / (2 * safe_float(sqrt_T)), safe_mul(r, K, exp_minus_rT, N_minus_d2))
            Rho = safe_mul(K, T, exp_minus_rT, N_d2) if opt_type == 'C' else -safe_mul(K, T, exp_minus_rT, N_minus_d2)

            # Second order greeks
            d1_d2 = safe_mul(d1, d2)
            Vanna = -safe_mul(n_d1, d2) / safe_float(sigma) if sigma > 0 else 0.0
            Volga = safe_mul(S, n_d1, sqrt_T, d1_d2) / safe_float(sigma) if sigma > 0 else 0.0
            two_r_T = safe_mul(2, r, T)
            d2_sigma_sqrt_T = safe_mul(d2, sigma_sqrt_T)
            two_r_T_minus_d2_sigma_sqrt_T = safe_add(two_r_T, -d2_sigma_sqrt_T)
            two_T_sigma_sqrt_T = safe_mul(2, T, sigma_sqrt_T) if T and T > 0 else 0.0
            Charm = (-safe_mul(n_d1, two_r_T_minus_d2_sigma_sqrt_T) / two_T_sigma_sqrt_T) if two_T_sigma_sqrt_T != 0 else 0.0
            Veta = safe_mul(S, n_d1, sqrt_T) * (safe_div(safe_mul(r, d1), sigma_sqrt_T) - safe_div(1 + d1_d2, 2 * safe_float(T))) if (sigma_sqrt_T != 0 and T and T > 0) else 0.0

            # Third order
            d1_over_sigma_sqrt_T = safe_div(d1, sigma_sqrt_T) if sigma_sqrt_T != 0 else 0.0
            Speed = (-safe_div(n_d1, safe_mul(S_squared, sigma_sqrt_T)) * (d1_over_sigma_sqrt_T + 1)) if (S_squared > 0 and sigma_sqrt_T > 0) else 0.0
            Zomma = (safe_mul(n_d1, d1_d2 - 1) / safe_mul(S, sigma_squared, sqrt_T)) if (S > 0 and sigma_squared > 0 and sqrt_T > 0) else 0.0
            Color = (-safe_div(n_d1, safe_mul(2, S, T, sigma_sqrt_T)) * (1 + safe_mul(two_r_T_minus_d2_sigma_sqrt_T, safe_div(d1, sigma_sqrt_T)))) if (S > 0 and T and T > 0 and sigma_sqrt_T > 0) else 0.0
            Ultima = (-safe_mul(S, n_d1, sqrt_T) / sigma_squared * (d1_d2 * (1 - d1_d2) + safe_mul(d1, d1) + safe_mul(d2, d2))) if (sigma_squared > 0 and sqrt_T > 0) else 0.0

            # Value metrics
            Intrinsic = 0.0
            if opt_type == 'C':
                Intrinsic = max(0.0, S - (K if K is not None else 0))
            elif opt_type == 'P':
                Intrinsic = max(0.0, (K if K is not None else 0) - S)
            Time_Value = markPrice - Intrinsic if markPrice is not None else 0.0
            Extrinsic = Time_Value
            Premium_Ratio = (markPrice / S * 100) if S > 0 else 0.0
            Strike_to_Spot = (K / S) if (K and S > 0) else 0.0
            Distance_to_Strike_Pct = ((S - K) / S * 100) if (K and S > 0) else 0.0

            # Moneyness
            Moneyness = (S / K) if (K and S > 0) else 0.0
            Log_Moneyness = ln_S_K
            Standardized_Moneyness = d2
            moneyness_cat = 'Unknown'
            if K and S > 0:
                ratio = S / K
                if ratio > 1.05:
                    moneyness_cat = 'Deep ITM Call/OTM Put'
                elif ratio > 1.00:
                    moneyness_cat = 'ITM Call/OTM Put'
                elif ratio >= 0.95:
                    moneyness_cat = 'ATM'
                elif ratio >= 0.90:
                    moneyness_cat = 'OTM Call/ITM Put'
                else:
                    moneyness_cat = 'Deep OTM Call/ITM Put'

            # Probabilities
            Prob_ITM = N_d2
            Prob_OTM = 1 - N_d2

            Breakeven = (K + markPrice) if opt_type == 'C' else (K - markPrice)
            # d2 breakeven
            d2_breakeven = 0.0
            try:
                if Breakeven > 0 and sigma_sqrt_T > 0:
                    d2_breakeven = (math.log(S / Breakeven) + (r - sigma_squared_half) * T) / sigma_sqrt_T
            except Exception:
                d2_breakeven = 0.0

            Prob_Profit = self._norm_cdf(d2_breakeven) if opt_type == 'C' else self._norm_cdf(-d2_breakeven)

            Expected_Value = exp_minus_rT * (S * N_d1 - K * N_d2) if opt_type == 'C' else exp_minus_rT * (K * N_minus_d2 - S * N_minus_d1)

            # Efficiency & leverage
            Leverage = (Delta * S / markPrice) if (markPrice and markPrice != 0) else 0.0
            Lambda = (Delta * S / theoretical_price) if (theoretical_price and theoretical_price != 0) else 0.0
            Return_on_Risk = ((S - Breakeven) / markPrice) if (opt_type == 'C' and markPrice) else (((Breakeven - S) / markPrice) if (opt_type == 'P' and markPrice) else 0.0)
            Risk_Adjusted_Return = (Expected_Value / markPrice - 1) if (markPrice and markPrice != 0) else 0.0

            # Mispricing
            Mispricing = ((markPrice - theoretical_price) / theoretical_price * 100) if (theoretical_price and theoretical_price != 0) else 0.0

            # Liquidity metrics
            Bid_Ask_Spread = ask_price - bid_price if (ask_price and bid_price) else 0.0
            Bid_Ask_Spread_Pct = (Bid_Ask_Spread / ((ask_price + bid_price) / 2) * 100) if (ask_price + bid_price > 0) else 0.0
            Mid_Price = (bid_price + ask_price) / 2 if (bid_price + ask_price) > 0 else markPrice
            Liquidity_Score = (bid_size + ask_size) / 2
            Volume_OI_Ratio = (volume24h / open_interest) if (open_interest and open_interest != 0) else 0.0
            Effective_Spread = ((ask_price * ask_size) - (bid_price * bid_size)) / (ask_size + bid_size) if (ask_size + bid_size) > 0 else 0.0
            Depth_Ratio = (bid_size / ask_size) if (ask_size and ask_size != 0) else 0.0
            Turnover_Ratio = (volume24h / (open_interest * markPrice)) if (open_interest and markPrice and open_interest != 0 and markPrice != 0) else 0.0

            # Time metrics
            Time_Decay_Rate = (Theta / markPrice) * 100 if (markPrice and markPrice != 0) else 0.0
            Theta_Per_Day = Theta / 365
            Days_to_Breakeven = (markPrice / (-Theta_Per_Day)) if Theta_Per_Day and Theta_Per_Day != 0 else None
            Theta_Premium_Pct = (Theta_Per_Day / markPrice) * 100 if (markPrice and markPrice != 0) else 0.0
            Annualized_Return = (Theta_Per_Day * 365) / markPrice * 100 if (markPrice and markPrice != 0) else 0.0

            # IV metrics
            IV_Spread = ask_iv - bid_iv if (ask_iv and bid_iv) else 0.0
            IV_Spread_Pct = (IV_Spread / markIv * 100) if (markIv and markIv != 0) else 0.0

            # Special ratios
            Vega_Theta_Ratio = (Vega / abs(Theta)) if Theta != 0 else None
            Delta_Gamma_Ratio = (abs(Delta) / Gamma) if Gamma != 0 else None
            Gamma_Vega_Ratio = (Gamma / Vega) if Vega != 0 else None

            # Optional additional greeks
            Vera = S * n_d1 * sqrt_T * d1_d2 / sigma if (sigma and sigma != 0) else 0.0
            Epsilon_Call = -S * T * N_d1
            Epsilon_Put = S * T * N_minus_d1

            # PIN Risk (Probability of being at the strike at expiration)
            # PIN Risk measures the concentration of max pain (gamma P&L) at the strike
            # Higher PIN Risk = higher probability of price ending near strike
            # Calculated as: probability density at strike (relative to total)
            PIN_Risk = 0.0
            if K and K > 0 and S > 0 and sigma > 0:
                # PDF of lognormal distribution at strike
                d_strike = (math.log(S / K) + (r + sigma_squared_half) * T) / sigma_sqrt_T if sigma_sqrt_T > 0 else 0
                pin_pdf = self._norm_pdf(d_strike) / (K * sigma_sqrt_T) if sigma_sqrt_T > 0 else 0
                # Normalize by S to get dimensionless metric
                PIN_Risk = (pin_pdf * S) if S > 0 else 0.0

            # Final bid/ask/mid computation for Node.js read-only layer
            # Invariant: All precomputed tables must be internally consistent at row level
            # No NULL bid/ask where mid is non-NULL.
            final_bid = bid_price if bid_price and bid_price > 0 else (markPrice - Bid_Ask_Spread / 2 if markPrice and Bid_Ask_Spread else markPrice)
            final_ask = ask_price if ask_price and ask_price > 0 else (markPrice + Bid_Ask_Spread / 2 if markPrice and Bid_Ask_Spread else markPrice)
            
            # Mid logic: (bid + ask) / 2. Fallback to theoretical_price if bid/ask unavailable.
            if final_bid and final_ask:
                final_mid = (final_bid + final_ask) / 2
            else:
                final_mid = theoretical_price
                # Maintain invariant: no NULL/0 bid/ask where mid is non-zero
                if final_mid and not final_bid: final_bid = final_mid
                if final_mid and not final_ask: final_ask = final_mid

            greeks = {
                'symbol': symbol,
                'S': S,
                'K': K,
                'sigma': sigma,
                'r': r,
                'q': q,
                'markPrice': markPrice,
                'markIv': markIv,
                'open_interest': open_interest,
                'volume24h': volume24h,
                'turnover24h': float(ticker[17]) if len(ticker) > 17 and ticker[17] is not None else 0,
                'bid1_size': bid_size,
                'ask1_size': ask_size,
                'change24h': float(ticker[25]) if len(ticker) > 25 and ticker[25] is not None else 0,
                'expiration_date': exp_date.strftime('%Y-%m-%d') if exp_date else None,
                'collection_datetime': coll_dt_str,
                'T': T,
                'option_type': opt_type,
                'theoretical_price': theoretical_price,
                'delta': Delta,
                'gamma': Gamma,
                'vega': Vega,
                'theta': Theta,
                'rho': Rho,
                'vanna': Vanna,
                'volga': Volga,
                'charm': Charm,
                'veta': Veta,
                'speed': Speed,
                'zomma': Zomma,
                'color': Color,
                'ultima': Ultima,
                'intrinsic_value': Intrinsic,
                'time_value': Time_Value,
                'extrinsic_value': Extrinsic,
                'premium_ratio': Premium_Ratio,
                'strike_to_spot': Strike_to_Spot,
                'distance_to_strike_pct': Distance_to_Strike_Pct,
                'moneyness': Moneyness,
                'log_moneyness': Log_Moneyness,
                'standardized_moneyness': Standardized_Moneyness,
                'moneyness_category': moneyness_cat,
                'prob_ITM': Prob_ITM,
                'prob_OTM': Prob_OTM,
                'breakeven': Breakeven,
                'd2_breakeven': d2_breakeven,
                'prob_profit': Prob_Profit,
                'expected_value': Expected_Value,
                'leverage': Leverage,
                'lambda_metric': Lambda,
                'mispricing': Mispricing,
                'bid_ask_spread': Bid_Ask_Spread,
                'bid_ask_spread_pct': Bid_Ask_Spread_Pct,
                'mid_price': Mid_Price,
                'liquidity_score': Liquidity_Score,
                'volume_oi_ratio': Volume_OI_Ratio,
                'effective_spread': Effective_Spread,
                'depth_ratio': Depth_Ratio,
                'turnover_ratio': Turnover_Ratio,
                'DTE': DTE,
                'time_decay_rate': Time_Decay_Rate,
                'theta_per_day': Theta_Per_Day,
                'days_to_breakeven': Days_to_Breakeven,
                'theta_premium_pct': Theta_Premium_Pct,
                'annualized_return': Annualized_Return,
                'IV_spread': IV_Spread,
                'IV_spread_pct': IV_Spread_Pct,
                'vega_theta_ratio': Vega_Theta_Ratio,
                'delta_gamma_ratio': Delta_Gamma_Ratio,
                'gamma_vega_ratio': Gamma_Vega_Ratio,
                'vera': Vera,
                'epsilon_call': Epsilon_Call,
                'epsilon_put': Epsilon_Put,
                'PIN_risk': PIN_Risk,
                'bid': final_bid,
                'ask': final_ask,
                'mid': final_mid
            }

            return greeks

        except Exception as e:
            logger.error(f"Error calculating option greeks for ticker {ticker}: {e}")
            return None

    def save_options_greeks_to_db(self, symbol, greeks_list, collection_id, collection_time):
        """Save list of greeks dicts into Options_Greeks table for the symbol"""
        try:
            symbol_folder = os.path.join(self.metrics_folder, f"Bybit_{symbol}")
            db_path = os.path.join(symbol_folder, f"Bybit_{symbol}_Op_Metrics.db")
            # Ensure schema
            self.create_metrics_database_schema(db_path)

            conn = sqlite3.connect(db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Safer dynamic insert: build column list from available keys to avoid mismatch
            expected_columns = [
                'symbol','S','K','sigma','r','q','expiration_date','collection_datetime','T','option_type',
                'theoretical_price','delta','gamma','vega','theta','rho','vanna','volga','charm','veta','speed',
                'zomma','color','ultima','intrinsic_value','time_value','extrinsic_value','premium_ratio',
                'strike_to_spot','distance_to_strike_pct','moneyness','log_moneyness','standardized_moneyness',
                'moneyness_category','prob_ITM','prob_OTM','breakeven','d2_breakeven','prob_profit','expected_value',
                'leverage','lambda_metric','mispricing','bid_ask_spread','bid_ask_spread_pct','mid_price',
                'liquidity_score','volume_oi_ratio','effective_spread','depth_ratio','turnover_ratio','DTE',
                'time_decay_rate','theta_per_day','days_to_breakeven','theta_premium_pct','annualized_return',
                'IV_spread','IV_spread_pct','vega_theta_ratio','delta_gamma_ratio','gamma_vega_ratio','vera',
                'epsilon_call','epsilon_put','PIN_risk',
                # raw fields for cumulative aggregation
                'markPrice','markIv','open_interest','volume24h','turnover24h','bid1_size','ask1_size','change24h',
                'bid', 'ask', 'mid'
            ]

            # Batch insert - prepare all rows at once
            batch_size = 100
            for i in range(0, len(greeks_list), batch_size):
                batch = greeks_list[i:i+batch_size]
                
                # Get columns from first record to ensure consistency
                cols = ['collection_id','time'] + [c for c in expected_columns if c in batch[0]]
                placeholders = ','.join('?' for _ in cols)
                insert_sql = f"INSERT INTO Options_Greeks ({', '.join(cols)}) VALUES ({placeholders})"

                values_list = []
                for g in batch:
                    vals = [collection_id, collection_time] + [g.get(c) for c in cols[2:]]
                    values_list.append(tuple(vals))
                
                try:
                    cursor.executemany(insert_sql, values_list)
                except Exception as e:
                    logger.warning(f"Failed to batch insert Options_Greeks for {symbol}: {e}")

            conn.commit()
            conn.close()
            # Kick off background computation of cumulative aggregates
            try:
                threading.Thread(target=self.compute_and_save_options_greeks_comulative, args=(symbol, collection_id, collection_time), daemon=True).start()
            except Exception as e:
                logger.warning(f"Failed to start cumulative computation thread for {symbol}: {e}")
            try:
                threading.Thread(target=self.compute_and_save_options_greeks_preaggregates, args=(symbol, collection_id), daemon=True).start()
            except Exception as e:
                logger.warning(f"Failed to start strike/expiry preaggregation thread for {symbol}: {e}")
            return True
        except Exception as e:
            logger.error(f"Error saving Options_Greeks for {symbol}: {e}")
            return False

    def compute_and_save_options_greeks_preaggregates(self, symbol, collection_id):
        try:
            symbol_folder = os.path.join(self.metrics_folder, f"Bybit_{symbol}")
            metrics_db = os.path.join(symbol_folder, f"Bybit_{symbol}_Op_Metrics.db")
            self.create_metrics_database_schema(metrics_db)

            asset = str(symbol).lower()
            conn = sqlite3.connect(metrics_db, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            cur.execute('DELETE FROM Options_Greeks_By_Strike WHERE asset = ?', (asset,))
            cur.execute('DELETE FROM Options_Greeks_By_Strike_Cumulative WHERE asset = ?', (asset,))
            cur.execute('DELETE FROM Options_Greeks_By_Expiry WHERE asset = ?', (asset,))
            cur.execute('DELETE FROM Options_Greeks_By_Strike_All_Expiries WHERE asset = ?', (asset,))

            strike_rows = cur.execute('''
                SELECT
                    expiration_date as date,
                    K as strike,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_oi,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as put_oi,
                    SUM(CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) as net_oi,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_gex,
                    SUM(CASE WHEN option_type='P' THEN -((CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END)) ELSE 0 END) as put_gex,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN delta IS NOT NULL THEN delta ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_dex,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN delta IS NOT NULL THEN delta ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as put_dex
                FROM Options_Greeks
                WHERE collection_id = ?
                  AND expiration_date IS NOT NULL
                  AND K IS NOT NULL
                GROUP BY expiration_date, K
            ''', (collection_id,)).fetchall()

            strike_values = []
            for r in strike_rows:
                call_oi = r['call_oi'] or 0
                put_oi = r['put_oi'] or 0
                net_oi = r['net_oi'] if r['net_oi'] is not None else (call_oi + put_oi)
                call_gex = r['call_gex'] or 0
                put_gex = r['put_gex'] or 0
                net_gex = call_gex + put_gex
                call_dex = r['call_dex'] or 0
                put_dex = r['put_dex'] or 0
                net_dex = call_dex + put_dex
                strike_values.append((asset, r['date'], r['strike'], call_oi, put_oi, net_oi, call_gex, put_gex, net_gex, call_dex, put_dex, net_dex))

            cur.executemany('''
                INSERT OR REPLACE INTO Options_Greeks_By_Strike (
                    asset, date, strike,
                    call_oi, put_oi, net_oi,
                    call_gex, put_gex, net_gex,
                    call_dex, put_dex, net_dex
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', strike_values)

            expiry_rows = cur.execute('''
                SELECT
                    expiration_date as expiry_date,
                    MIN(DTE) as dte,
                    COUNT(*) as contract_count,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_oi,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as put_oi,
                    SUM(CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) as net_oi,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_gamma,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as put_gamma,
                    SUM(CASE WHEN gamma IS NOT NULL THEN gamma * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as total_gamma,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_gex,
                    SUM(CASE WHEN option_type='P' THEN -((CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END)) ELSE 0 END) as put_gex,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN delta IS NOT NULL THEN delta ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_dex,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN delta IS NOT NULL THEN delta ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as put_dex
                FROM Options_Greeks
                WHERE collection_id = ?
                  AND expiration_date IS NOT NULL
                GROUP BY expiration_date
                ORDER BY expiration_date
            ''', (collection_id,)).fetchall()

            expiry_values = []
            for r in expiry_rows:
                call_oi = r['call_oi'] or 0
                put_oi = r['put_oi'] or 0
                net_oi = r['net_oi'] if r['net_oi'] is not None else (call_oi + put_oi)
                call_gex = r['call_gex'] or 0
                put_gex = r['put_gex'] or 0
                net_gex = call_gex + put_gex
                call_dex = r['call_dex'] or 0
                put_dex = r['put_dex'] or 0
                net_dex = call_dex + put_dex
                expiry_values.append((
                    asset, r['expiry_date'], r['dte'], r['contract_count'],
                    call_oi, put_oi, net_oi,
                    r['call_gamma'] or 0, r['put_gamma'] or 0, r['total_gamma'] or 0,
                    call_gex, put_gex, net_gex,
                    call_dex, put_dex, net_dex
                ))

            cur.executemany('''
                INSERT OR REPLACE INTO Options_Greeks_By_Expiry (
                    asset, expiry_date, dte, contract_count,
                    call_oi, put_oi, net_oi,
                    call_gamma, put_gamma, total_gamma,
                    call_gex, put_gex, net_gex,
                    call_dex, put_dex, net_dex
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', expiry_values)

            all_rows = cur.execute('''
                SELECT
                    K as strike,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_oi,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as put_oi,
                    SUM(CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) as net_oi,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_gex,
                    SUM(CASE WHEN option_type='P' THEN -((CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END)) ELSE 0 END) as put_gex,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN delta IS NOT NULL THEN delta ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_dex,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN delta IS NOT NULL THEN delta ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as put_dex
                FROM Options_Greeks
                WHERE collection_id = ?
                  AND expiration_date >= date('now', '-1 day')
                  AND (
                    expiration_date > date('now')
                    OR (
                      expiration_date = date('now')
                      AND strftime('%H', 'now') < '08'
                    )
                  )
                  AND K IS NOT NULL
                GROUP BY K
            ''', (collection_id,)).fetchall()

            all_values = []
            for r in all_rows:
                call_oi = r['call_oi'] or 0
                put_oi = r['put_oi'] or 0
                net_oi = r['net_oi'] if r['net_oi'] is not None else (call_oi + put_oi)
                call_gex = r['call_gex'] or 0
                put_gex = r['put_gex'] or 0
                net_gex = call_gex + put_gex
                call_dex = r['call_dex'] or 0
                put_dex = r['put_dex'] or 0
                net_dex = call_dex + put_dex
                all_values.append((asset, r['strike'], call_oi, put_oi, net_oi, call_gex, put_gex, net_gex, call_dex, put_dex, net_dex))

            cur.executemany('''
                INSERT OR REPLACE INTO Options_Greeks_By_Strike_All_Expiries (
                    asset, strike,
                    call_oi, put_oi, net_oi,
                    call_gex, put_gex, net_gex,
                    call_dex, put_dex, net_dex
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', all_values)

            # Populate Options_Volatility table for direct SELECT * from Node.js
            cur.execute('DELETE FROM Options_Volatility WHERE asset = ?', (asset,))
            vol_rows = cur.execute('''
                SELECT
                    expiration_date as expiry,
                    K as strike,
                    option_type as type,
                    markIv as iv,
                    delta,
                    gamma,
                    vega,
                    bid,
                    ask,
                    mid
                FROM Options_Greeks
                WHERE collection_id = ?
                  AND expiration_date IS NOT NULL
                  AND K IS NOT NULL
            ''', (collection_id,)).fetchall()
            
            vol_values = [(asset, r['expiry'], r['strike'], r['type'], r['iv'], r['delta'], r['gamma'], r['vega'], r['bid'], r['ask'], r['mid']) for r in vol_rows]
            cur.executemany('''
                INSERT OR REPLACE INTO Options_Volatility (
                    asset, expiry, strike, type, iv, delta, gamma, vega, bid, ask, mid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', vol_values)

            active_dates = [r['expiry_date'] for r in expiry_rows if r['expiry_date'] is not None]
            if active_dates:
                placeholders = ','.join('?' for _ in active_dates)
                cum_rows = cur.execute(f'''
                    SELECT
                        K as strike,
                        SUM(CASE WHEN option_type='C' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_oi,
                        SUM(CASE WHEN option_type='P' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as put_oi,
                        SUM(CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) as net_oi,
                        SUM(CASE WHEN option_type='C' THEN (CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_gex,
                        SUM(CASE WHEN option_type='P' THEN -((CASE WHEN gamma IS NOT NULL THEN gamma ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END)) ELSE 0 END) as put_gex,
                        SUM(CASE WHEN option_type='C' THEN (CASE WHEN delta IS NOT NULL THEN delta ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as call_dex,
                        SUM(CASE WHEN option_type='P' THEN (CASE WHEN delta IS NOT NULL THEN delta ELSE 0 END) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as put_dex
                    FROM Options_Greeks
                    WHERE collection_id = ?
                      AND expiration_date IN ({placeholders})
                      AND K IS NOT NULL
                    GROUP BY K
                ''', (collection_id, *active_dates)).fetchall()

                cum_values = []
                for r in cum_rows:
                    call_oi = r['call_oi'] or 0
                    put_oi = r['put_oi'] or 0
                    net_oi = r['net_oi'] if r['net_oi'] is not None else (call_oi + put_oi)
                    call_gex = r['call_gex'] or 0
                    put_gex = r['put_gex'] or 0
                    net_gex = call_gex + put_gex
                    call_dex = r['call_dex'] or 0
                    put_dex = r['put_dex'] or 0
                    net_dex = call_dex + put_dex
                    cum_values.append((asset, 'CUMULATIVE', r['strike'], call_oi, put_oi, net_oi, call_gex, put_gex, net_gex, call_dex, put_dex, net_dex))

                cur.executemany('''
                    INSERT OR REPLACE INTO Options_Greeks_By_Strike_Cumulative (
                        asset, date, strike,
                        call_oi, put_oi, net_oi,
                        call_gex, put_gex, net_gex,
                        call_dex, put_dex, net_dex
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', cum_values)

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error computing strike/expiry preaggregates for {symbol}: {e}")
            try:
                conn.close()
            except Exception:
                pass
            return False

    def save_metrics_to_db(self, symbol, metrics, collection_date, collection_time):
        """
        Сохранить рассчитанные метрики в базу данных
        """
        try:
            # Получить путь к базе данных метрик для этого символа
            symbol_folder = os.path.join(self.metrics_folder, f"Bybit_{symbol}")
            db_path = os.path.join(symbol_folder, f"Bybit_{symbol}_Op_Metrics.db")
            
            # Создать схему базы данных, если она не существует
            self.create_metrics_database_schema(db_path)
            
            # Подключиться к базе данных с увеличенным timeout
            conn = sqlite3.connect(db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Проверить, существуют ли уже данные с такой же датой и временем
            cursor.execute('''
                SELECT COUNT(*) FROM metrics_data 
                WHERE collection_date = ? AND collection_time = ?
            ''', (collection_date, collection_time))
            
            duplicate_count = cursor.fetchone()[0]
            if duplicate_count > 0:
                conn.close()
                return True
            
            # Normalize volatility metrics to percentages if needed (ensure consistency)
            # Create a shallow copy to safely modify
            metrics_norm = metrics.copy()
            vol_keys = ['implied_volatility', 'historical_volatility', 'realized_volatility',
                        'avg_IV', 'avg_IV_Calls', 'avg_IV_Puts', 'median_IV']
            
            for k in vol_keys:
                val = metrics_norm.get(k)
                if val is not None and isinstance(val, (int, float)) and 0 < val < 5:
                    metrics_norm[k] = val * 100

            # Подготовить данные для вставки
            values = (
                collection_date, collection_time,
                metrics_norm.get('underlying_price', 0),
                metrics_norm.get('futures_price', 0),
                metrics_norm.get('total_delta', 0),
                metrics_norm.get('total_gamma', 0),
                metrics_norm.get('total_vega', 0),
                metrics_norm.get('total_theta', 0),
                metrics_norm.get('total_rho', 0),
                metrics_norm.get('call_delta', 0),
                metrics_norm.get('put_delta', 0),
                metrics_norm.get('call_gamma', 0),
                metrics_norm.get('put_gamma', 0),
                metrics_norm.get('vanna', 0),
                metrics_norm.get('charm', 0),
                metrics_norm.get('vomma', 0),
                metrics_norm.get('veta', 0),
                metrics_norm.get('speed', 0),
                metrics_norm.get('color', 0),
                metrics_norm.get('ultima', 0),
                metrics_norm.get('zomma', 0),
                metrics_norm.get('gex', 0),
                metrics_norm.get('dex', 0),
                metrics_norm.get('intrinsic_value', 0),
                metrics_norm.get('extrinsic_value', 0),
                metrics_norm.get('option_premium', 0),
                metrics_norm.get('moneyness', 0),
                metrics_norm.get('bid_ask_spread', 0),
                metrics_norm.get('implied_volatility', 0),
                metrics_norm.get('historical_volatility', 0),
                metrics_norm.get('realized_volatility', 0),
                metrics_norm.get('volatility_skew_metric', 0),
                metrics_norm.get('term_structure', 0),
                metrics_norm.get('smile_slope', 0),
                metrics_norm.get('smile_curvature', 0),
                metrics_norm.get('volatility_skew', 0),
                metrics_norm.get('skewness', 0),
                metrics_norm.get('kurtosis', 0),
                metrics_norm.get('put_call_ratio', 0),
                metrics_norm.get('open_interest_total', 0),
                metrics_norm.get('volume_total', 0),
                metrics_norm.get('market_depth', 0),
                metrics_norm.get('order_imbalance', 0),
                metrics_norm.get('order_flow', 0),
                metrics_norm.get('smile_slope_raw', 0),
                metrics_norm.get('IV_pct', 0),
                metrics_norm.get('RV_pct', 0),
                metrics_norm.get('HV_pct', 0),
                metrics_norm.get('Sell_Score', 0),
                metrics_norm.get('Buy_Score', 0),
                metrics_norm.get('Gap', 0),
                metrics_norm.get('w_iv_sell', 0),
                metrics_norm.get('w_rv_sell', 0),
                metrics_norm.get('w_hv_sell', 0),
                metrics_norm.get('w_iv_buy', 0),
                metrics_norm.get('w_rv_buy', 0),
                metrics_norm.get('w_hv_buy', 0)
            )
            
            # Вставить данные метрик
            insert_query = '''
                INSERT INTO metrics_data (
                    collection_date, 
                    collection_time, 
                    underlying_price,
                    futures_price,
                    total_delta, 
                    total_gamma, 
                    total_vega, 
                    total_theta, 
                    total_rho,
                    call_delta, 
                    put_delta, 
                    call_gamma, 
                    put_gamma,
                    vanna, 
                    charm, 
                    vomma, 
                    veta,
                    speed, 
                    color, 
                    ultima, 
                    zomma,
                    gex, 
                    dex, 
                    intrinsic_value, 
                    extrinsic_value,
                    option_premium, 
                    moneyness, 
                    bid_ask_spread,
                    implied_volatility, 
                    historical_volatility, 
                    realized_volatility,
                    volatility_skew_metric, 
                    term_structure, 
                    smile_slope, 
                    smile_curvature, 
                    volatility_skew,
                    skewness, 
                    kurtosis, 
                    put_call_ratio,
                    open_interest_total, 
                    volume_total, 
                    market_depth, 
                    order_imbalance, 
                    order_flow,
                    smile_slope_raw,
                    IV_pct,
                    RV_pct,
                    HV_pct,
                    Sell_Score,
                    Buy_Score,
                    Gap,
                    w_iv_sell,
                    w_rv_sell,
                    w_hv_sell,
                    w_iv_buy,
                    w_rv_buy,
                    w_hv_buy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''

            cursor.execute(insert_query, values)
            conn.commit()
            
            # Verify that data was saved by checking the last inserted row
            cursor.execute('''
                SELECT COUNT(*) FROM metrics_data 
                WHERE collection_date = ? AND collection_time = ?
            ''', (collection_date, collection_time))
            
            saved_count = cursor.fetchone()[0]
            conn.close()
            
            return saved_count > 0
            
        except Exception as e:
            logger.error(f"Error saving metrics to database for {symbol}: {e}")
            return False

    def compute_and_save_options_greeks_comulative(self, symbol, collection_id, collection_time):
        """
        Compute aggregated/cumulative metrics from Options_Greeks for the given collection_id and save into Options_Greeks_Comulative.
        Runs in background thread when invoked from save_options_greeks_to_db.
        """
        try:
            # Paths
            symbol_folder = os.path.join(self.metrics_folder, f"Bybit_{symbol}")
            metrics_db = os.path.join(symbol_folder, f"Bybit_{symbol}_Op_Metrics.db")
            api_db = os.path.join(self.data_folder, f"Bybit_{symbol}", f"Bybit_{symbol}_API.db")

            # Ensure schema exists
            self.create_metrics_database_schema(metrics_db)

            # Get base_coin from api DB options_data
            base_coin = None
            try:
                conn_api = sqlite3.connect(api_db, timeout=30.0)
                conn_api.row_factory = sqlite3.Row
                cur_api = conn_api.cursor()
                cur_api.execute('SELECT base_coin FROM options_data WHERE id = ?', (collection_id,))
                r = cur_api.fetchone()
                if r:
                    base_coin = r[0]
                conn_api.close()
            except Exception:
                base_coin = None

            conn = sqlite3.connect(metrics_db, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            # Aggregate using SQL
            agg_sql = '''
                SELECT
                    COUNT(*) as n_total,
                    SUM(CASE WHEN option_type='C' THEN 1 ELSE 0 END) as n_calls,
                    SUM(CASE WHEN option_type='P' THEN 1 ELSE 0 END) as n_puts,
                    SUM(CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) as sum_OI,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_OI_calls,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_OI_puts,
                    SUM(CASE WHEN volume_oi_ratio IS NOT NULL THEN volume_oi_ratio * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_volume,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN volume_oi_ratio IS NOT NULL THEN volume_oi_ratio * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_volume_calls,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN volume_oi_ratio IS NOT NULL THEN volume_oi_ratio * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_volume_puts,
                    SUM(CASE WHEN turnover_ratio IS NOT NULL THEN turnover_ratio * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_turnover,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN turnover_ratio IS NOT NULL THEN turnover_ratio * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_turnover_calls,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN turnover_ratio IS NOT NULL THEN turnover_ratio * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_turnover_puts,
                    SUM(CASE WHEN delta IS NOT NULL THEN delta * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Delta_OI,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN delta IS NOT NULL THEN delta * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_Delta_OI_calls,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN delta IS NOT NULL THEN delta * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_Delta_OI_puts,
                    SUM(CASE WHEN gamma IS NOT NULL THEN gamma * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Gamma_OI,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN gamma IS NOT NULL THEN gamma * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_Gamma_OI_calls,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN gamma IS NOT NULL THEN gamma * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_Gamma_OI_puts,
                    SUM(CASE WHEN vega IS NOT NULL THEN vega * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Vega_OI,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN vega IS NOT NULL THEN vega * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_Vega_OI_calls,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN vega IS NOT NULL THEN vega * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_Vega_OI_puts,
                    SUM(CASE WHEN theta IS NOT NULL THEN theta * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Theta_OI,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN theta IS NOT NULL THEN theta * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_Theta_OI_calls,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN theta IS NOT NULL THEN theta * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_Theta_OI_puts,
                    SUM(CASE WHEN vanna IS NOT NULL THEN vanna * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Vanna_OI,
                    SUM(CASE WHEN volga IS NOT NULL THEN volga * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Volga_OI,
                    SUM(CASE WHEN charm IS NOT NULL THEN charm * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Charm_OI,
                    SUM(CASE WHEN veta IS NOT NULL THEN veta * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Veta_OI,
                    SUM(CASE WHEN speed IS NOT NULL THEN speed * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Speed_OI,
                    SUM(CASE WHEN zomma IS NOT NULL THEN zomma * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Zomma_OI,
                    SUM(CASE WHEN color IS NOT NULL THEN color * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Color_OI,
                    SUM(CASE WHEN ultima IS NOT NULL THEN ultima * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Ultima_OI,
                    SUM(CASE WHEN markIv IS NOT NULL THEN markIv * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_IV_OI,
                    SUM(CASE WHEN option_type='C' THEN (CASE WHEN markIv IS NOT NULL THEN markIv * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_IV_OI_calls,
                    SUM(CASE WHEN option_type='P' THEN (CASE WHEN markIv IS NOT NULL THEN markIv * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) ELSE 0 END) as sum_IV_OI_puts,
                    SUM(CASE WHEN markPrice IS NOT NULL THEN markPrice * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_markPrice_OI,
                    SUM(CASE WHEN intrinsic_value IS NOT NULL THEN intrinsic_value * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_Intrinsic_OI,
                    SUM(CASE WHEN time_value IS NOT NULL THEN time_value * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_TimeValue_OI,
                    SUM(CASE WHEN bid_ask_spread IS NOT NULL THEN bid_ask_spread * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_BidAskSpread_OI,
                    SUM(CASE WHEN bid_ask_spread_pct IS NOT NULL THEN bid_ask_spread_pct * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_BidAskSpreadPct_OI,
                    SUM(CASE WHEN liquidity_score IS NOT NULL THEN liquidity_score ELSE 0 END) as sum_bid_size,
                    SUM(CASE WHEN liquidity_score IS NOT NULL THEN liquidity_score ELSE 0 END) as sum_ask_size,
                    AVG(S) as S_current,
                    SUM(CASE WHEN option_type='C' AND intrinsic_value>0 THEN 1 ELSE 0 END) as n_ITM_calls,
                    SUM(CASE WHEN option_type='C' AND ABS(S - K)/S < 0.02 THEN 1 ELSE 0 END) as n_ATM_calls,
                    SUM(CASE WHEN option_type='C' AND intrinsic_value=0 AND NOT (ABS(S - K)/S < 0.02) THEN 1 ELSE 0 END) as n_OTM_calls,
                    SUM(CASE WHEN option_type='P' AND intrinsic_value>0 THEN 1 ELSE 0 END) as n_ITM_puts,
                    SUM(CASE WHEN option_type='P' AND ABS(S - K)/S < 0.02 THEN 1 ELSE 0 END) as n_ATM_puts,
                    SUM(CASE WHEN option_type='P' AND intrinsic_value=0 AND NOT (ABS(S - K)/S < 0.02) THEN 1 ELSE 0 END) as n_OTM_puts,
                    SUM(CASE WHEN intrinsic_value>0 AND option_type='C' THEN open_interest ELSE 0 END) as sum_OI_ITM_calls,
                    SUM(CASE WHEN intrinsic_value=0 AND option_type='C' THEN open_interest ELSE 0 END) as sum_OI_OTM_calls,
                    SUM(CASE WHEN intrinsic_value>0 AND option_type='P' THEN open_interest ELSE 0 END) as sum_OI_ITM_puts,
                    SUM(CASE WHEN intrinsic_value=0 AND option_type='P' THEN open_interest ELSE 0 END) as sum_OI_OTM_puts,
                    SUM(CASE WHEN DTE <= 7 THEN open_interest ELSE 0 END) as sum_OI_DTE_7d,
                    SUM(CASE WHEN DTE > 7 AND DTE <= 30 THEN open_interest ELSE 0 END) as sum_OI_DTE_30d,
                    SUM(CASE WHEN DTE > 30 AND DTE <= 90 THEN open_interest ELSE 0 END) as sum_OI_DTE_90d,
                    SUM(CASE WHEN DTE > 90 AND DTE <= 180 THEN open_interest ELSE 0 END) as sum_OI_DTE_180d,
                    SUM(CASE WHEN DTE > 180 THEN open_interest ELSE 0 END) as sum_OI_DTE_180plus,
                    SUM(DTE * open_interest) as sum_DTE_OI,
                    SUM(ABS(mispricing) * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END)) as sum_abs_Mispricing_OI,
                    SUM(CASE WHEN time_value>0 THEN 1 ELSE 0 END) as n_active_contracts,
                    SUM(CASE WHEN change24h IS NOT NULL THEN change24h * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_change24h_OI,
                    SUM(CASE WHEN volume_oi_ratio IS NOT NULL THEN volume_oi_ratio * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_VolumeOI_OI,
                    SUM(CASE WHEN PIN_risk IS NOT NULL THEN PIN_risk * (CASE WHEN open_interest IS NOT NULL THEN open_interest ELSE 0 END) ELSE 0 END) as sum_PINrisk_OI,
                    SUM(CASE WHEN moneyness < 0.90 AND option_type='P' THEN open_interest ELSE 0 END) as sum_OI_deep_OTM_puts,
                    SUM(CASE WHEN moneyness > 1.10 AND option_type='C' THEN open_interest ELSE 0 END) as sum_OI_deep_OTM_calls,
                    SUM(open_interest) as total_open_interest
                FROM Options_Greeks
                WHERE collection_id = ?
            '''

            cur.execute(agg_sql, (collection_id,))
            row = cur.fetchone()
            if not row:
                conn.close()
                return False

            cols = [d[0] for d in cur.description]
            agg = dict(zip(cols, row))

            # Derived metrics
            put_call_OI_ratio = (agg.get('sum_OI_puts') or 0) / (agg.get('sum_OI_calls') or 1)
            total_notional_OI = (agg.get('sum_OI') or 0) * (agg.get('S_current') or 0)

            # IV stats - fetch all IV data in one query
            cur.execute('''
                SELECT markIv, option_type, volume24h
                FROM Options_Greeks 
                WHERE collection_id = ? AND markIv IS NOT NULL
            ''', (collection_id,))
            iv_data = cur.fetchall()
            
            median_iv = None
            iv_std = None
            iv_range = None
            avg_iv = None
            avg_iv_calls = None
            avg_iv_puts = None
            total_volume_24h = None
            total_call_volume = None
            total_put_volume = None
            put_call_volume_ratio = None
            
            if iv_data:
                iv_vals = [r[0] for r in iv_data if r[0] is not None]
                if iv_vals:
                    iv_vals_sorted = sorted(iv_vals)
                    n = len(iv_vals_sorted)
                    median_iv = iv_vals_sorted[n//2] if n%2==1 else (iv_vals_sorted[n//2-1]+iv_vals_sorted[n//2])/2
                    mean_iv = sum(iv_vals_sorted)/n
                    iv_std = math.sqrt(sum((x-mean_iv)**2 for x in iv_vals_sorted)/n)
                    iv_range = max(iv_vals_sorted)-min(iv_vals_sorted)
                    
                    # Normalize IV metrics to percentages
                    avg_iv = mean_iv * 100
                    median_iv = median_iv * 100
                    iv_std = iv_std * 100
                    iv_range = iv_range * 100
                
                # Calculate call and put IV averages and volumes from single query result
                call_ivs = [r[0] for r in iv_data if r[1] == 'C']
                put_ivs = [r[0] for r in iv_data if r[1] == 'P']
                
                if call_ivs:
                    avg_iv_calls = (sum(call_ivs) / len(call_ivs)) * 100
                if put_ivs:
                    avg_iv_puts = (sum(put_ivs) / len(put_ivs)) * 100
            
            # Get volume metrics in single query
            cur.execute('''
                SELECT 
                    SUM(volume24h) as total_vol,
                    SUM(CASE WHEN option_type = "C" THEN volume24h ELSE 0 END) as call_vol,
                    SUM(CASE WHEN option_type = "P" THEN volume24h ELSE 0 END) as put_vol
                FROM Options_Greeks 
                WHERE collection_id = ? AND volume24h IS NOT NULL
            ''', (collection_id,))
            vol_row = cur.fetchone()
            if vol_row:
                total_volume_24h = vol_row[0]
                total_call_volume = vol_row[1]
                total_put_volume = vol_row[2]
                
                if total_call_volume is not None and total_call_volume > 0 and total_put_volume is not None:
                    put_call_volume_ratio = total_put_volume / total_call_volume

            # Calculate realized and historical volatility from markPrice changes
            # We'll use the standard deviation of log returns as a proxy for realized volatility
            realized_volatility = None
            historical_volatility = None
            try:
                cur.execute('''
                    SELECT markPrice, collection_datetime 
                    FROM Options_Greeks 
                    WHERE collection_id = ? AND markPrice IS NOT NULL 
                    ORDER BY collection_datetime
                ''', (collection_id,))
                price_data = cur.fetchall()
                
                if len(price_data) > 1:
                    # Calculate log returns
                    log_returns = []
                    for i in range(1, len(price_data)):
                        if price_data[i][0] > 0 and price_data[i-1][0] > 0:
                            log_return = math.log(price_data[i][0] / price_data[i-1][0])
                            log_returns.append(log_return)
                    
                    if len(log_returns) > 0:
                        # Calculate standard deviation (realized volatility proxy)
                        mean_return = sum(log_returns) / len(log_returns)
                        variance = sum((r - mean_return) ** 2 for r in log_returns) / len(log_returns)
                        # Annualize (already appears to be in percentage scale based on user data 83 vs 0.83)
                        realized_volatility = math.sqrt(variance) * math.sqrt(365)
                        
                        # Calculate historical volatility as a moving average of realized volatility
                        # Using a longer window (e.g., 30 periods) for historical volatility
                        window = min(30, len(log_returns))
                        if window > 1:
                            # Calculate historical volatility as average of recent realized volatilities
                            recent_variances = []
                            for j in range(len(log_returns) - window + 1):
                                window_returns = log_returns[j:j + window]
                                window_mean = sum(window_returns) / len(window_returns)
                                window_variance = sum((r - window_mean) ** 2 for r in window_returns) / len(window_returns)
                                recent_variances.append(window_variance)
                            
                            if recent_variances:
                                avg_variance = sum(recent_variances) / len(recent_variances)
                                # Annualize
                                historical_volatility = math.sqrt(avg_variance) * math.sqrt(365)
                            else:
                                historical_volatility = realized_volatility
                        else:
                            historical_volatility = realized_volatility
                else:
                    # If not enough data points, use the same value for both but log a warning
                    if len(price_data) == 1:
                        logger.debug(f"Insufficient price data for volatility calculation for {symbol}, using zero values")
            except Exception as e:
                logger.warning(f"Could not calculate realized/historical volatility for {symbol}: {e}")

            # Prepare insert
            insert_cols = (
                'collection_id','time','base_coin'
            )
            insert_vals = [collection_id, collection_time, base_coin]

            # append aggregated columns in same order as in table definition
            for k in cols:
                insert_cols += (k,)
                insert_vals.append(agg.get(k))

            # Calculate IVR metrics
            ivr_metrics = None
            if avg_iv is not None and avg_iv > 0:
                ivr_metrics = self.calculate_IVR(symbol, avg_iv, metrics_db)
            
            # Calculate Max Pain
            max_pain = self.calculate_max_pain(symbol, collection_id, metrics_db)
            
            # Calculate enhanced IVR metrics
            enhanced_ivr_metrics = None
            if avg_iv is not None and avg_iv > 0:
                # We need current_rv and current_hv for the enhanced IVR calculation
                # These were calculated earlier in the function
                enhanced_ivr_metrics = self.calculate_enhanced_ivr_scores(
                    symbol, avg_iv, realized_volatility, historical_volatility, metrics_db)
            
            # Extract IVR values or use None
            ivr = ivr_metrics.get('IVR') if ivr_metrics else None
            ivp = ivr_metrics.get('IVP') if ivr_metrics else None
            iv_z_score = ivr_metrics.get('IV_Z_Score') if ivr_metrics else None
            iv_52w_min = ivr_metrics.get('IV_52w_min') if ivr_metrics else None
            iv_52w_max = ivr_metrics.get('IV_52w_max') if ivr_metrics else None
            iv_52w_mean = ivr_metrics.get('IV_52w_mean') if ivr_metrics else None
            iv_52w_std = ivr_metrics.get('IV_52w_std') if ivr_metrics else None
            iv_data_points = ivr_metrics.get('IV_data_points') if ivr_metrics else None
            
            # Extract enhanced IVR values or use None/defaults
            iv_pct = enhanced_ivr_metrics.get('IV_pct') if enhanced_ivr_metrics else None
            rv_pct = enhanced_ivr_metrics.get('RV_pct') if enhanced_ivr_metrics else None
            hv_pct = enhanced_ivr_metrics.get('HV_pct') if enhanced_ivr_metrics else None
            sell_score = enhanced_ivr_metrics.get('Sell_Score') if enhanced_ivr_metrics else None
            buy_score = enhanced_ivr_metrics.get('Buy_Score') if enhanced_ivr_metrics else None
            gap = enhanced_ivr_metrics.get('Gap') if enhanced_ivr_metrics else None
            w_iv_sell = enhanced_ivr_metrics.get('w_iv_sell') if enhanced_ivr_metrics else None
            w_rv_sell = enhanced_ivr_metrics.get('w_rv_sell') if enhanced_ivr_metrics else None
            w_hv_sell = enhanced_ivr_metrics.get('w_hv_sell') if enhanced_ivr_metrics else None
            w_iv_buy = enhanced_ivr_metrics.get('w_iv_buy') if enhanced_ivr_metrics else None
            w_rv_buy = enhanced_ivr_metrics.get('w_rv_buy') if enhanced_ivr_metrics else None
            w_hv_buy = enhanced_ivr_metrics.get('w_hv_buy') if enhanced_ivr_metrics else None

            # derived placeholders
            insert_cols += ('put_call_OI_ratio','total_notional_OI','total_volume_24h','total_call_volume','total_put_volume','put_call_volume_ratio','avg_IV','avg_IV_Calls','avg_IV_Puts','median_IV','IV_Std_Dev','IV_Range','IVR','IVP','IV_Z_Score','IV_52w_min','IV_52w_max','IV_52w_mean','IV_52w_std','IV_data_points','max_pain','realized_volatility','historical_volatility','IV_pct','RV_pct','HV_pct','Sell_Score','Buy_Score','Gap','w_iv_sell','w_rv_sell','w_hv_sell','w_iv_buy','w_rv_buy','w_hv_buy')
            insert_vals += [put_call_OI_ratio, total_notional_OI, total_volume_24h, total_call_volume, total_put_volume, put_call_volume_ratio, avg_iv, avg_iv_calls, avg_iv_puts, median_iv, iv_std, iv_range, ivr, ivp, iv_z_score, iv_52w_min, iv_52w_max, iv_52w_mean, iv_52w_std, iv_data_points, max_pain, realized_volatility, historical_volatility, iv_pct, rv_pct, hv_pct, sell_score, buy_score, gap, w_iv_sell, w_rv_sell, w_hv_sell, w_iv_buy, w_rv_buy, w_hv_buy]

            placeholders = ','.join('?' for _ in insert_cols)
            insert_sql = f"INSERT INTO Options_Greeks_Comulative ({', '.join(insert_cols)}) VALUES ({placeholders})"
            cur.execute(insert_sql, tuple(insert_vals))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error computing/saving Options_Greeks_Comulative for {symbol}: {e}")
            return False

# Add execution code when running as script
if __name__ == "__main__":
    try:
        # Create analyzer instance
        analyzer = OptionsAnalyzer("Config_Bybit_OP_Metrics.yaml")
        
        # Get configuration values
        calculation_interval = analyzer.config.get('calculation_interval', 5)  # Default to 5 minutes
        # 7 тикеров по умолчанию
        default_tickers = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'MNT']
        tickers = analyzer.config.get('tickers', default_tickers)
        if not tickers or not isinstance(tickers, list) or len(tickers) < 2:
            logger.warning(f"Config does not specify enough tickers, using default: {default_tickers}")
            tickers = default_tickers
        
        # Check if running in single-run mode (TEST_RUN=1)
        test_run = os.environ.get('TEST_RUN', '0') == '1'
        
        while True:
            # Get current date and time for collection
            start_time = datetime.datetime.now()
            collection_date = start_time.strftime("%Y-%m-%d")
            collection_time = start_time.strftime("%H:%M:%S")
            
            # Calculate next calculation time
            next_calculation = start_time + datetime.timedelta(minutes=calculation_interval)
            next_calculation_time = next_calculation.strftime("%Y-%m-%d %H:%M:%S")
            
            # Track results for each ticker
            results = {}
            save_results = {}
            
            # Process each ticker
            for symbol in tickers:
                # Ensure database schema is up to date
                try:
                    symbol_folder = os.path.join(analyzer.metrics_folder, f"Bybit_{symbol}")
                    metrics_db = os.path.join(symbol_folder, f"Bybit_{symbol}_Op_Metrics.db")
                    analyzer.create_metrics_database_schema(metrics_db)
                except Exception as e:
                    logger.warning(f"Could not update database schema for {symbol}: {e}")
                
                # Fetch latest options data
                options_data, options_details, options_tickers = analyzer.fetch_latest_options_data(symbol)
                
                if not options_data:
                    logger.warning(f"No options_data found for {symbol}. DB may be empty or missing.")
                    results[symbol] = 0
                    save_results[symbol] = False
                    continue
                if not options_details:
                    logger.warning(f"No options_details found for {symbol}. No contracts for latest collection.")
                    results[symbol] = 0
                    save_results[symbol] = False
                    continue
                if not options_tickers:
                    logger.warning(f"No options_tickers found for {symbol}. No tickers for latest collection.")
                    results[symbol] = 0
                    save_results[symbol] = False
                    continue
                # Calculate metrics
                metrics = analyzer.calculate_metrics(options_data, options_details, options_tickers)
                if not metrics:
                    logger.warning(f"No metrics calculated for {symbol}. Check input data and calculation logic.")
                    results[symbol] = 0
                    save_results[symbol] = False
                    continue
                # Calculate enhanced IVR metrics
                try:
                    # Get the database path
                    symbol_folder = os.path.join(analyzer.metrics_folder, f"Bybit_{symbol}")
                    db_path = os.path.join(symbol_folder, f"Bybit_{symbol}_Op_Metrics.db")
                    # Get implied volatility from metrics
                    implied_volatility = metrics.get('implied_volatility', 0)
                    realized_volatility = metrics.get('realized_volatility', 0)
                    historical_volatility = metrics.get('historical_volatility', 0)
                    if implied_volatility > 0:
                        # Calculate enhanced IVR metrics
                        enhanced_ivr_metrics = analyzer.calculate_enhanced_ivr_scores(
                            symbol, implied_volatility, realized_volatility, historical_volatility, db_path)
                        # Add enhanced IVR metrics to the main metrics dictionary
                        if enhanced_ivr_metrics:
                            metrics.update(enhanced_ivr_metrics)
                except Exception as e:
                    logger.warning(f"Could not calculate enhanced IVR metrics for {symbol}: {e}")
                # Count non-zero metrics
                non_zero_metrics = sum(1 for value in metrics.values() if value != 0)
                results[symbol] = non_zero_metrics
                # Save metrics to database
                success = analyzer.save_metrics_to_db(symbol, metrics, collection_date, collection_time)
                # Save detailed Options_Greeks per ticker
                try:
                    # collection_id: try to extract from options_data tuple (first element is id)
                    collection_id = options_data[0] if options_data and len(options_data) > 0 else None
                    greeks_list = []
                    for t in options_tickers:
                        g = analyzer.calculate_option_greeks_for_ticker(t, collection_datetime=start_time, options_details=options_details)
                        if g:
                            greeks_list.append(g)
                    if greeks_list:
                        greeks_saved = analyzer.save_options_greeks_to_db(symbol, greeks_list, collection_id, collection_time)
                        logger.info(f"  Greeks: {len(greeks_list)} / {len(options_tickers)} contracts processed for {symbol}")
                    else:
                        greeks_saved = False
                        logger.warning(f"  Greeks: 0 contracts processed for {symbol}")
                except Exception as e:
                    logger.error(f"Error processing Options_Greeks for {symbol}: {e}")
                    greeks_saved = False
                # Save individual contract GEX/DEX data
                if 'contract_gex_dex_data' in metrics and metrics['contract_gex_dex_data']:
                    # Add underlying price to each contract record
                    contract_data = metrics['contract_gex_dex_data']
                    for contract in contract_data:
                        # Prefer the unified 'underlying_price' from metrics; fall back to ticker fields
                        underlying_price = metrics.get('underlying_price')
                        if underlying_price is None or underlying_price == 0:
                            # Look for underlying_price inside options_tickers list (index 15) or use markPrice (index 9)
                            underlying_price = None
                            if options_tickers:
                                for tk in options_tickers:
                                    if len(tk) > 15 and tk[15] is not None and tk[15] > 0:
                                        underlying_price = float(tk[15])
                                        break
                                if underlying_price is None:
                                    for tk in options_tickers:
                                        if len(tk) > 9 and tk[9] is not None and tk[9] > 0:
                                            underlying_price = float(tk[9])
                                            break
                        contract['underlying_price'] = underlying_price
                    contract_success = analyzer.save_contract_gex_dex_to_db(
                        symbol, contract_data, collection_date, collection_time)
                    save_results[symbol] = success and contract_success
                else:
                    save_results[symbol] = success
            
            # Calculate execution time
            end_time = datetime.datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # Print only the essential information
            print(f"Tickers: {', '.join(tickers)}")
            for symbol, metrics_count in results.items():
                save_status = "saved" if save_results.get(symbol, False) else "not saved"
                print(f"{symbol}: {metrics_count} metrics ({save_status})")
            print(f"Time: {execution_time:.2f}s")
            print(f"Next in: {calculation_interval}m at {next_calculation_time}")
            print("-" * 30)
            
            # Exit if test_run is enabled
            if test_run:
                logger.info("Test run completed. Exiting.")
                break
            
            # Wait for the specified interval before next calculation
            time.sleep(calculation_interval * 60)
        
        # Exit normally
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
