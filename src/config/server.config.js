/**
 * Server Configuration for Dynamic Asset Loading
 * Конфигурация сервера для динамической загрузки активов
 */

const sqlite3 = require('sqlite3').verbose();
const path = require('path');

/**
 * Unified Asset Configuration (Node.js version)
 * Копия из assets.config.js для использования в Node.js
 */
const ASSETS = [
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
    enabled: true
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
    enabled: true
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
    enabled: true
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
    enabled: true
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
    enabled: true
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
    enabled: true
  }
];

const FIELD_MAPPING = {
  metrics: {
    timestamp: 'timestamp',
    price: 'Close',
    open: 'Open',
    high: 'High',
    low: 'Low',
    volume: 'Volume'
  },
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
 * Генерация универсальных API endpoints для всех активов
 * 
 * @param {object} app - Express app instance
 */
function setupAssetEndpoints(app) {
  const activeAssets = ASSETS.filter(a => a.enabled);
  
  activeAssets.forEach(asset => {
    const endpoint = `/api/${asset.id}/latest`;
    const historyEndpoint = `/api/${asset.id}/latest/:count`;
    
    console.log(`[SETUP] Creating endpoint: ${endpoint}`);
    
    // Latest data endpoint
    app.get(endpoint, (req, res) => {
      const db = new sqlite3.Database(asset.dbPath, (err) => {
        if (err) {
          console.error(`[${asset.symbol}] DB connection error:`, err.message);
          return res.status(500).json({ error: 'Database connection failed' });
        }
      });
      
      // Получить последнюю запись метрик
      const metricsQuery = `SELECT * FROM ${asset.tables.metrics} ORDER BY id DESC LIMIT 1`;
      
      db.get(metricsQuery, [], (err, metricsData) => {
        if (err) {
          console.error(`[${asset.symbol}] Metrics query error:`, err.message);
          db.close();
          return res.status(500).json({ error: 'Metrics query failed' });
        }
        
        if (!metricsData) {
          db.close();
          return res.status(404).json({ error: 'No data found' });
        }
        
        // Получить греки
        const greeksQuery = `SELECT * FROM ${asset.tables.greeks} ORDER BY id DESC LIMIT 1`;
        
        db.get(greeksQuery, [], (err2, greeksData) => {
          if (err2) {
            console.error(`[${asset.symbol}] Greeks query error:`, err2.message);
          }
          
          // Собрать результат
          const result = {
            asset: asset.id,
            exchange: asset.exchange,
            ticker: asset.ticker,
            symbol: asset.symbol,
            timestamp: metricsData[FIELD_MAPPING.metrics.timestamp] || Date.now(),
            price: metricsData[FIELD_MAPPING.metrics.price] || 0,
            open: metricsData[FIELD_MAPPING.metrics.open] || 0,
            high: metricsData[FIELD_MAPPING.metrics.high] || 0,
            low: metricsData[FIELD_MAPPING.metrics.low] || 0,
            volume: metricsData[FIELD_MAPPING.metrics.volume] || 0
          };
          
          // Добавить греки, если есть
          if (greeksData) {
            Object.keys(FIELD_MAPPING.greeks).forEach(apiField => {
              const dbField = FIELD_MAPPING.greeks[apiField];
              result[apiField] = greeksData[dbField] || 0;
            });
          }
          
          db.close();
          res.json(result);
        });
      });
    });
    
    // History endpoint
    app.get(historyEndpoint, (req, res) => {
      const count = parseInt(req.params.count) || 100;
      const db = new sqlite3.Database(asset.dbPath, (err) => {
        if (err) {
          console.error(`[${asset.symbol}] DB connection error:`, err.message);
          return res.status(500).json({ error: 'Database connection failed' });
        }
      });
      
      const query = `SELECT * FROM ${asset.tables.metrics} ORDER BY id DESC LIMIT ?`;
      
      db.all(query, [count], (err, rows) => {
        if (err) {
          console.error(`[${asset.symbol}] History query error:`, err.message);
          db.close();
          return res.status(500).json({ error: 'History query failed' });
        }
        
        const history = rows.reverse().map(row => ({
          timestamp: row[FIELD_MAPPING.metrics.timestamp] || 0,
          price: row[FIELD_MAPPING.metrics.price] || 0,
          open: row[FIELD_MAPPING.metrics.open] || 0,
          high: row[FIELD_MAPPING.metrics.high] || 0,
          low: row[FIELD_MAPPING.metrics.low] || 0,
          volume: row[FIELD_MAPPING.metrics.volume] || 0
        }));
        
        db.close();
        res.json({
          asset: asset.id,
          exchange: asset.exchange,
          ticker: asset.ticker,
          symbol: asset.symbol,
          count: history.length,
          data: history
        });
      });
    });
  });
}

/**
 * Endpoint для получения списка всех активов
 */
function setupAssetsListEndpoint(app) {
  app.get('/api/assets', (req, res) => {
    const activeAssets = ASSETS.filter(a => a.enabled).map(a => ({
      id: a.id,
      exchange: a.exchange,
      ticker: a.ticker,
      symbol: a.symbol,
      name: a.name,
      enabled: a.enabled
    }));
    
    res.json({
      count: activeAssets.length,
      assets: activeAssets
    });
  });
}

/**
 * Endpoint для получения списка бирж
 */
function setupExchangesEndpoint(app) {
  app.get('/api/exchanges', (req, res) => {
    const exchanges = [...new Set(ASSETS.filter(a => a.enabled).map(a => a.exchange))];
    
    res.json({
      count: exchanges.length,
      exchanges: exchanges.map(ex => ({
        name: ex,
        assets: ASSETS.filter(a => a.exchange === ex && a.enabled).map(a => ({
          id: a.id,
          ticker: a.ticker,
          symbol: a.symbol
        }))
      }))
    });
  });
}

module.exports = {
  ASSETS,
  FIELD_MAPPING,
  setupAssetEndpoints,
  setupAssetsListEndpoint,
  setupExchangesEndpoint
};