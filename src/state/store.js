/**
 * Centralized State Management (Store).
 * Contract: only data, only setters/getters/subscribe. No DOM, no timers, no business logic.
 */

// Initial exchange configuration
const initialExchangeData = {
    bybit: {
        name: 'Bybit',
        assets: [
            { symbol: 'BTC/USDT', name: 'Spot', price: 0, change: 0, changePct: 0, real: true },
            { symbol: 'ETH/USDT', name: 'Spot', price: 0, change: 0, changePct: 0, real: false },
            { symbol: 'SOL/USDT', name: 'Spot', price: 0, change: 0, changePct: 0, real: false },
            { symbol: 'XRP/USDT', name: 'Spot', price: 0, change: 0, changePct: 0, real: false },
            { symbol: 'MNT/USDT', name: 'Spot', price: 0, change: 0, changePct: 0, real: false },
            { symbol: 'DOGE/USDT', name: 'Spot', price: 0, change: 0, changePct: 0, real: false }
        ]
    },
    okx: {
        name: 'OKX',
        assets: [
            { symbol: 'BTC-USDT', name: 'Spot', price: 0, change: 0, changePct: 0, stub: true },
            { symbol: 'ETH-USDT', name: 'Spot', price: 0, change: 0, changePct: 0, stub: true }
        ]
    },
    bingx: {
        name: 'BingX',
        assets: [
            { symbol: 'BTC-USDT', name: 'BTC/USDT', price: 0, change: 0, changePct: 0, stub: true },
            { symbol: 'ETH-USDT', name: 'ETH/USDT', price: 0, change: 0, changePct: 0, stub: true }
        ]
    }
};

/**
 * Creates an empty asset data structure.
 * @returns {Object}
 */
const createEmptyAssetData = () => ({
    name: 'SPOT Price',
    price: 0,
    spotPrice: 0,
    change: 0,
    changePct: 0,
    gex: 0,
    dex: 0,
    iv: 0,
    ivr: 0,
    callOi: 0,
    putOi: 0,
    // Greeks
    sum_Delta_OI: 0,
    sum_Gamma_OI: 0,
    sum_Vega_OI: 0,
    sum_Theta_OI: 0,
    sum_Vanna_OI: 0,
    sum_Charm_OI: 0,
    sum_Volga_OI: 0,
    sum_Veta_OI: 0,
    sum_Speed_OI: 0,
    sum_Zomma_OI: 0,
    sum_Color_OI: 0,
    sum_Ultima_OI: 0,
    total_rho: 0,
    maxPain: 0,
    underlying_price: 0,
    priceHistory: [],
    spotHistory: [],
    futuresPrice: 0,
    futuresHistory: [],
    chain: [],
    gexChart: []
});

let state = {
    data: {
        BTCUSDT: createEmptyAssetData(),
        ETHUSDT: createEmptyAssetData(),
        SOLUSDT: createEmptyAssetData(),
        XRPUSDT: createEmptyAssetData(),
        MNTUSDT: createEmptyAssetData(),
        DOGEUSDT: createEmptyAssetData()
    },
    exchangeData: initialExchangeData,
    currentExchange: 'bybit',
    activeTicker: 'BTCUSDT',
    currentTab: 'crypto',
    // GEX UI State
    gexMode: 'single', // 'single' or 'cumulative'
    selectedExpiry: null // Date string
};

const listeners = new Set();

export const store = {
    /**
     * Updates the global state.
     * @param {Object} newData - Partial state update.
     * @param {string} [source] - Optional source of the update for debugging.
     */
    setState: (newData, source = 'unknown') => {
        // Simple shallow merge for top-level, but deep merge for data if needed
        // For simplicity during decomposition, we'll do a shallow merge of the state object
        state = { ...state, ...newData };

        // Synchronize exchangeData.bybit.assets with state.data
        if (newData.data) {
            console.log('[store] Synchronizing exchangeData from state.data');
            state.exchangeData.bybit.assets.forEach(asset => {
                // Extract symbol key from asset (e.g., 'BTC/USDT' -> 'BTC')
                const symbolKey = asset.symbol.split('/')[0].toUpperCase();
                const assetData = state.data[symbolKey];
                if (assetData) {
                    console.debug(`[store] Sync ${symbolKey}:`, {
                        price: assetData.spotPrice || assetData.price,
                        changePct: assetData.changePct
                    });
                    asset.price = assetData.spotPrice || assetData.price || 0;
                    asset.changePct = assetData.changePct || 0;
                    asset.change = 0; // Will calculate from history if available
                }
            });
        }

        // Notify subscribers
        listeners.forEach(listener => listener(state));
    },

    /**
     * Internal state getter.
     * @returns {Object}
     */
    getStateInternal: () => state,

    /**
     * Subscribe to state changes.
     * @param {Function} listener - Callback receiving the new state.
     * @returns {Function} - Function to unsubscribe.
     */
    subscribe: (listener) => {
        listeners.add(listener);
        return () => listeners.delete(listener);
    }
};
