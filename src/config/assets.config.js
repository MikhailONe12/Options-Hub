/**
 * Unified Asset Configuration
 * Единая конфигурация активов для всех бирж
 * 
 * Для добавления нового актива:
 * 1. Добавить объект в массив ASSETS
 * 2. Указать exchange, ticker, dbPath, tableName
 * 3. Перезапустить сервер - всё остальное подключится автоматически
 */

export const ASSETS = [
  // ========== BYBIT ==========
  {
    id: 'bybit-btc',
    exchange: 'Bybit',
    ticker: 'BTCUSDT',
    symbol: 'BTC',
    name: 'Bitcoin',
    dbPath: 'E:/10.12.2025/Option_Hub/0_Data/Bybit_metrics/Bybit_BTC/Bybit_BTC_Op_Metrics.db',
    tables: {
      metrics: 'Options_Data_1d',
      greeks: 'Options_Greeks_Comulative'
    },
    enabled: true,
    color: '#F7931A'
  },
  {
    id: 'bybit-eth',
    exchange: 'Bybit',
    ticker: 'ETHUSDT',
    symbol: 'ETH',
    name: 'Ethereum',
    dbPath: 'E:/10.12.2025/Option_Hub/0_Data/Bybit_metrics/Bybit_ETH/Bybit_ETH_Op_Metrics.db',
    tables: {
      metrics: 'Options_Data_1d',
      greeks: 'Options_Greeks_Comulative'
    },
    enabled: true,
    color: '#627EEA'
  },
  {
    id: 'bybit-sol',
    exchange: 'Bybit',
    ticker: 'SOLUSDT',
    symbol: 'SOL',
    name: 'Solana',
    dbPath: 'E:/10.12.2025/Option_Hub/0_Data/Bybit_metrics/Bybit_SOL/Bybit_SOL_Op_Metrics.db',
    tables: {
      metrics: 'Options_Data_1d',
      greeks: 'Options_Greeks_Comulative'
    },
    enabled: true,
    color: '#14F195'
  },
  {
    id: 'bybit-xrp',
    exchange: 'Bybit',
    ticker: 'XRPUSDT',
    symbol: 'XRP',
    name: 'XRP',
    dbPath: 'E:/10.12.2025/Option_Hub/0_Data/Bybit_metrics/Bybit_XRP/Bybit_XRP_Op_Metrics.db',
    tables: {
      metrics: 'Options_Data_1d',
      greeks: 'Options_Greeks_Comulative'
    },
    enabled: true,
    color: '#2e3036'
  },
  {
    id: 'bybit-mnt',
    exchange: 'Bybit',
    ticker: 'MNTUSDT',
    symbol: 'MNT',
    name: 'Mantle',
    dbPath: 'E:/10.12.2025/Option_Hub/0_Data/Bybit_metrics/Bybit_MNT/Bybit_MNT_Op_Metrics.db',
    tables: {
      metrics: 'Options_Data_1d',
      greeks: 'Options_Greeks_Comulative'
    },
    enabled: true,
    color: '#000000'
  },
  {
    id: 'bybit-doge',
    exchange: 'Bybit',
    ticker: 'DOGEUSDT',
    symbol: 'DOGE',
    name: 'Dogecoin',
    dbPath: 'E:/10.12.2025/Option_Hub/0_Data/Bybit_metrics/Bybit_DOGE/Bybit_DOGE_Op_Metrics.db',
    tables: {
      metrics: 'Options_Data_1d',
      greeks: 'Options_Greeks_Comulative'
    },
    enabled: true,
    color: '#CB9800'
  },
  
  // ========== DERIBIT (example) ==========
  {
    id: 'deribit-btc',
    exchange: 'Deribit',
    ticker: 'BTC-PERPETUAL',
    symbol: 'BTC',
    name: 'Bitcoin',
    dbPath: 'G:/Option_Hub/0_Data/Deribit_metrics/Deribit_BTC_Op_Metrics.db',
    tables: {
      metrics: 'Options_Data_1d',
      greeks: 'Options_Greeks_Comulative'
    },
    enabled: false, // Отключен, пока нет данных
    color: '#F7931A'
  },
  
  // ========== BINANCE (example) ==========
  {
    id: 'binance-btc',
    exchange: 'Binance',
    ticker: 'BTCUSDT',
    symbol: 'BTC',
    name: 'Bitcoin',
    dbPath: 'G:/Option_Hub/0_Data/Binance_metrics/Binance_BTC_Op_Metrics.db',
    tables: {
      metrics: 'Options_Data_1d',
      greeks: 'Options_Greeks_Comulative'
    },
    enabled: false,
    color: '#F3BA2F'
  }
];

/**
 * Маппинг полей базы данных на API поля
 * Если структура таблиц отличается между биржами - настроить здесь
 */
export const FIELD_MAPPING = {
  // Базовые поля из Options_Data_1d
  metrics: {
    timestamp: 'timestamp',
    price: 'Close',
    open: 'Open',
    high: 'High',
    low: 'Low',
    volume: 'Volume'
  },
  
  // Греки из Options_Greeks_Comulative
  greeks: {
    ivr: 'IVR',
    iv: 'IV',
    gex: 'GEX',
    dex: 'DEX',
    gamma: 'Gamma',
    delta: 'Delta',
    vega: 'Vega',
    theta: 'Theta'
  }
};

/**
 * Поля для инициализации в window.data
 */
export const DEFAULT_DATA_STRUCTURE = {
  spotHistory: [],
  futuresHistory: [],
  priceHistory: [],
  price: 0,
  spotPrice: 0,
  futuresPrice: 0,
  timestamp: 0,
  
  // Greeks
  ivr: 0,
  iv: 0,
  gex: 0,
  dex: 0,
  gamma: 0,
  delta: 0,
  vega: 0,
  theta: 0,
  
  // OHLCV
  open: 0,
  high: 0,
  low: 0,
  volume: 0,
  
  // Вычисляемые
  change: 0,
  changePct: 0
};

/**
 * Получить все активные активы
 */
export function getActiveAssets() {
  return ASSETS.filter(a => a.enabled);
}

/**
 * Получить актив по тикеру
 */
export function getAssetByTicker(ticker) {
  return ASSETS.find(a => a.ticker === ticker && a.enabled);
}

/**
 * Получить актив по ID
 */
export function getAssetById(id) {
  return ASSETS.find(a => a.id === id);
}

/**
 * Получить активы по бирже
 */
export function getAssetsByExchange(exchange) {
  return ASSETS.filter(a => a.exchange === exchange && a.enabled);
}

/**
 * Получить все уникальные биржи
 */
export function getExchanges() {
  return [...new Set(ASSETS.filter(a => a.enabled).map(a => a.exchange))];
}

/**
 * Получить все уникальные символы
 */
export function getSymbols() {
  return [...new Set(ASSETS.filter(a => a.enabled).map(a => a.symbol))];
}

export default {
  ASSETS,
  FIELD_MAPPING,
  DEFAULT_DATA_STRUCTURE,
  getActiveAssets,
  getAssetByTicker,
  getAssetById,
  getAssetsByExchange,
  getExchanges,
  getSymbols
};