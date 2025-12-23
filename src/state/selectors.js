// selectors.js – Pure data projection layer (PCF‑STRICT compliance)
// Returns only raw, non‑derived values from the store.
// Presentation‑specific logic has been moved to block renderers.

import { store } from './store.js';

/** Helper: Normalize symbol to store key format. */
function normalizeSymbol(ticker) {
    if (!ticker) return null;
    // Remove 'USDT' suffix if present
    const normalized = ticker.replace('USDT', '').toUpperCase();
    return normalized;
}

/** Helper: No‑op formatting function retained for compatibility (returns raw value). */
function formatValue(v, decimals = 2) {
    return v; // No formatting – raw value only
}

/** Data Access Layer (Selectors). */
export const selectors = {
    getState: () => store.getStateInternal(),

    getMarketData: (state, symbol) => state.data[symbol] || null,

    getExchangeAssets: (state, exchange) => state.exchangeData[exchange]?.assets || [],

    getActiveTicker: (state) => state.activeTicker,

    getCurrentExchange: (state) => state.currentExchange || 'Bybit',

    getCurrentTab: (state) => state.currentTab || 'crypto',

    // ---- Raw metric selectors (no UI logic) ----
    selectAssetMeta: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const data = state.data[baseSymbol];
        if (!data) return null;
        return {
            ticker: symbol || state.activeTicker,
            price: data.underlying_price || data.mark_price || 0,
            change: data.price_change_24h || 0,
            changePercent: data.price_change_percent_24h || 0,
            lastUpdate: state.lastUpdateTime
        };
    },

    selectGexMetric: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const data = state.data[baseSymbol];
        if (!data) return null;
        const val = data.sum_Gamma_OI || 0;
        return { value: val };
    },

    selectOiMetric: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const data = state.data[baseSymbol];
        if (!data) return null;
        const val = data.sum_OpenInterest || 0;
        return { value: val };
    },

    selectGreekData: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const data = state.data[baseSymbol];
        if (!data) return null;
        return {
            firstOrder: [
                { label: 'Delta', value: data.sum_Delta_OI || 0 },
                { label: 'Gamma', value: data.sum_Gamma_OI || 0 },
                { label: 'Vega', value: data.sum_Vega_OI || 0 },
                { label: 'Theta', value: data.sum_Theta_OI || 0 }
            ],
            secondOrder: [
                { label: 'Vanna', value: data.sum_Vanna_OI || 0 },
                { label: 'Charm', value: data.sum_Charm_OI || 0 },
                { label: 'Vomma', value: data.sum_Volga_OI || 0 }
            ]
        };
    },

    selectMaxPainData: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const data = state.data[baseSymbol];
        if (!data) return null;
        const maxPain = data.max_pain || 0;
        const price = data.underlying_price || 0;
        const distance = price - maxPain;
        const pct = maxPain ? (distance / maxPain) * 100 : 0;
        return { maxPain, price, distance, pct };
    },

    selectPcRatioData: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const data = state.data[baseSymbol];
        if (!data) return null;
        const calls = data.call_oi || data.callOi || 0;
        const puts = data.put_oi || data.putOi || 0;
        const total = calls + puts || 1;
        const ratio = puts / (calls || 1);
        const net = calls - puts;
        const callPct = (calls / total) * 100;
        const putPct = (puts / total) * 100;
        return { calls, puts, total, ratio, net, callPct, putPct };
    },

    selectUsdRubMetric: (state) => {
        const d = state.data['USDRUB'];
        const price = d?.price || d?.spotPrice || 0;
        return { value: price };
    },

    selectImoexMetric: (state) => {
        const d = state.data['IMOEX'];
        const price = d?.price || d?.close || 0;
        return { value: price };
    },

    // ---- Data selectors (raw only) ----
    selectPriceComparisonData: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const d = state.data[baseSymbol];
        if (!d) return null;
        return {
            spotHistory: d.spotHistory || [],
            futuresHistory: d.futuresHistory || [],
            spotPrice: d.underlying_price || d.price || 0,
            futPrice: d.futuresPrice || 0,
            sellScore: d.Sell_Score ?? d.sell_score ?? d.ivr ?? 0,
            buyScore: d.Buy_Score ?? d.buy_score ?? 0,
            signal: 0 // placeholder – calculation moved to renderer
        };
    },

    selectGexTableData: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const data = state.data[baseSymbol];
        if (!data || !data.chain || !data.chain.length) return null;
        const allExpiries = [...new Set(data.chain.map(c => c.expiry))].sort();
        const expirations = allExpiries.map(expiry => {
            const items = data.chain.filter(i => i.expiry === expiry);
            const totalGamma = items.reduce((sum, i) => sum + parseFloat(i.net_gex || i.gex || 0), 0);
            const expDate = new Date(expiry);
            const now = new Date();
            const dte = Math.ceil((expDate - now) / (1000 * 60 * 60 * 24));
            return { expiry, dte, totalGamma, isWeekly: expDate.getDay() === 5 };
        });
        return {
            expirations,
            currentMode: state.gexMode || 'single',
            selectedExpiry: state.selectedExpiry || allExpiries[0]
        };
    },

    selectSmileData: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const assetData = state.data[baseSymbol];
        if (!assetData || !assetData.chain || !assetData.chain.length) {
            return {
                calls: [],
                puts: [],
                atm: null,
                meta: {
                    spot: assetData?.price || 0,
                    expiration: state.selectedExpiry || 'N/A',
                    hasData: false,
                    lastUpdated: Date.now()
                }
            };
        }
        const spot = assetData.underlying_price || assetData.price || 0;
        const targetExpiry = state.selectedExpiry || [...new Set(assetData.chain.map(c => c.expiry))].sort()[0];
        const points = assetData.chain
            .filter(item => item.expiry === targetExpiry)
            .map(item => ({
                strike: parseFloat(item.strike),
                callIv: parseFloat(item.call_iv || item.iv || 0),
                putIv: parseFloat(item.put_iv || item.iv || 0)
            }))
            .sort((a, b) => a.strike - b.strike);
        if (points.length === 0) {
            return {
                calls: [],
                puts: [],
                atm: null,
                meta: { spot, expiration: targetExpiry, hasData: false, lastUpdated: Date.now() }
            };
        }
        const calls = [];
        const puts = [];
        let atmPoint = null;
        let minDiff = Infinity;
        points.forEach(p => {
            const moneyness = p.strike / (spot || 1);
            const diff = Math.abs(p.strike - spot);
            if (diff < minDiff) {
                minDiff = diff;
                atmPoint = { strike: p.strike, iv: (p.callIv + p.putIv) / 2, moneyness };
            }
            if (p.callIv > 0 && !isNaN(p.callIv)) {
                calls.push({ strike: p.strike, iv: p.callIv, moneyness });
            }
            if (p.putIv > 0 && !isNaN(p.putIv)) {
                puts.push({ strike: p.strike, iv: p.putIv, moneyness });
            }
        });
        return {
            calls,
            puts,
            atm: atmPoint,
            meta: { spot, expiration: targetExpiry, hasData: true, lastUpdated: Date.now() }
        };
    },

    // Matrix data selector (raw only, unchanged)
    selectMatrixData: (state, symbol) => {
        const baseSymbol = symbol ? symbol.replace('USDT', '').toUpperCase() : state.activeTicker;
        const data = state.data[baseSymbol];
        if (!data || !data.chain || !data.chain.length) return null;
        const strategy = 'log'; // Default strategy
        const mode = state.gexMode || 'single';
        const targetExpiry = state.selectedExpiry || [...new Set(data.chain.map(c => c.expiry))].sort()[0];
        const filteredChain = data.chain.filter(item => {
            if (mode === 'single') return item.expiry === targetExpiry;
            if (mode === 'cumulative') return item.expiry <= targetExpiry;
            return true;
        });
        const strikes = [...new Set(filteredChain.map(c => parseFloat(c.strike)))].sort((a, b) => b - a);
        const expiries = [...new Set(filteredChain.map(c => c.expiry))].sort();
        const cells = {};
        const maxVals = { dex: 0, gex: 0, netOi: 0, oi: 0 };
        const rawValues = { gex: [], dex: [], netOi: [], oi: [] };
        strikes.forEach(strike => {
            const strikeItems = filteredChain.filter(item => parseFloat(item.strike) === strike);
            const gex = strikeItems.reduce((sum, item) => sum + parseFloat(item.net_gex || item.gex || 0), 0);
            const dex = strikeItems.reduce((sum, item) => sum + parseFloat(item.net_dex || item.dex || 0), 0);
            const netOi = strikeItems.reduce((sum, item) => sum + parseFloat(item.net_oi || 0), 0);
            const callOi = strikeItems.reduce((sum, item) => sum + parseFloat(item.call_oi || 0), 0);
            const putOi = strikeItems.reduce((sum, item) => sum + parseFloat(item.put_oi || 0), 0);
            const maxOi = Math.max(callOi, putOi);
            const key = `${strike}:${expiries[0]}`;
            cells[key] = {
                gex: { value: gex, sign: Math.sign(gex) },
                dex: { value: dex, sign: Math.sign(dex) },
                netOi: { value: netOi, sign: Math.sign(netOi) },
                callOi: { value: callOi },
                putOi: { value: putOi },
                strike,
                expiry: expiries[0]
            };
            rawValues.gex.push(Math.abs(gex));
            rawValues.dex.push(Math.abs(dex));
            rawValues.netOi.push(Math.abs(netOi));
            rawValues.oi.push(maxOi);
        });
        maxVals.gex = Math.max(...rawValues.gex, 1);
        maxVals.dex = Math.max(...rawValues.dex, 1);
        maxVals.netOi = Math.max(...rawValues.netOi, 1);
        maxVals.oi = Math.max(...rawValues.oi, 1);
        let maxNetGexValue = -Infinity;
        let minNetGexValue = Infinity;
        let maxCallGexStrike = 0;
        let maxPutGexStrike = 0;
        Object.keys(cells).forEach(key => {
            const cell = cells[key];
            cell.gex.intensity = STRATEGIES[strategy](Math.abs(cell.gex.value), 0, maxVals.gex);
            cell.dex.intensity = STRATEGIES[strategy](Math.abs(cell.dex.value), 0, maxVals.dex);
            cell.gex.level = Math.ceil(cell.gex.intensity * 10);
            cell.dex.level = Math.ceil(cell.dex.intensity * 10);
            cell.gex.width = (Math.abs(cell.gex.value) / maxVals.gex) * 100;
            cell.dex.width = (Math.abs(cell.dex.value) / maxVals.dex) * 100;
            cell.netOi.width = (Math.abs(cell.netOi.value) / maxVals.netOi) * 100;
            cell.callOi.width = (cell.callOi.value / maxVals.oi) * 100;
            cell.putOi.width = (cell.putOi.value / maxVals.oi) * 100;
            if (cell.gex.value > maxNetGexValue) {
                maxNetGexValue = cell.gex.value;
                maxCallGexStrike = cell.strike;
            }
            if (cell.gex.value < minNetGexValue) {
                minNetGexValue = cell.gex.value;
                maxPutGexStrike = cell.strike;
            }
        });
        return {
            ticker: symbol || state.activeTicker,
            spotPrice: data.underlying_price || data.price,
            strikes,
            expiries,
            targetExpiry,
            mode,
            cells,
            maxVals,
            keyLevels: {
                resistance: maxCallGexStrike,
                support: maxPutGexStrike
            }
        };
    }
};
