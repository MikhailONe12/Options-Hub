const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbPaths = {
    btc: path.join('E:', '10.12.2025', 'Option_Hub', '0_Data', 'Bybit_metrics', 'Bybit_BTC', 'Bybit_BTC_Op_Metrics.db'),
    eth: path.join('E:', '10.12.2025', 'Option_Hub', '0_Data', 'Bybit_metrics', 'Bybit_ETH', 'Bybit_ETH_Op_Metrics.db'),
    sol: path.join('E:', '10.12.2025', 'Option_Hub', '0_Data', 'Bybit_metrics', 'Bybit_SOL', 'Bybit_SOL_Op_Metrics.db'),
    xrp: path.join('E:', '10.12.2025', 'Option_Hub', '0_Data', 'Bybit_metrics', 'Bybit_XRP', 'Bybit_XRP_Op_Metrics.db'),
    mnt: path.join('E:', '10.12.2025', 'Option_Hub', '0_Data', 'Bybit_metrics', 'Bybit_MNT', 'Bybit_MNT_Op_Metrics.db'),
    doge: path.join('E:', '10.12.2025', 'Option_Hub', '0_Data', 'Bybit_metrics', 'Bybit_DOGE', 'Bybit_DOGE_Op_Metrics.db'),

    // MOEX Data
    imoex: path.join('E:', '10.12.2025', 'Option_Hub', '2_Development', '!MOEX Fast API', 'TestNEWdata', 'MOEX_IMOEX', 'MOEX_IMOEX_API.db')
};

const dbMap = {};

function createDb(p, label) {
    const d = new sqlite3.Database(p, sqlite3.OPEN_READONLY, e => {
        if (e) console.error(`[${label.toUpperCase()}] DB error:`, e.message);
        else console.log(`[${label.toUpperCase()}] DB connected`);
    });
    d.configure('busyTimeout', 30000);
    return d;
}

function initDbConnections() {
    Object.entries(dbPaths).forEach(([k, v]) => {
        dbMap[k] = createDb(v, k);
    });
}

function openDbForAsset(asset) {
    // Return existing connection from map (singleton pattern)
    if (dbMap[asset]) return dbMap[asset];

    // Fallback/Lazy init if not in map but path exists
    if (dbPaths[asset]) {
        dbMap[asset] = createDb(dbPaths[asset], asset);
        return dbMap[asset];
    }

    return null;
}

module.exports = {
    dbPaths,
    dbMap,
    initDbConnections,
    openDbForAsset
};
