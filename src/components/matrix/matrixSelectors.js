import { selectors } from '../../state/selectors.js';

/**
 * Matrix Normalization Strategies.
 */
const STRATEGIES = {
    linear: (val, min, max) => {
        if (max === min) return 0;
        const normalized = (val - min) / (max - min);
        return Math.max(0, Math.min(1, normalized));
    },

    log: (val, min, max) => {
        if (val <= 0) return 0;
        const logVal = Math.log10(val + 1);
        const logMax = Math.log10(max + 1);
        const logMin = Math.log10(min + 1);
        if (logMax === logMin) return 0;
        const normalized = (logVal - logMin) / (logMax - logMin);
        return Math.max(0, Math.min(1, normalized));
    },

    percentile: (val, sortedValues) => {
        if (!sortedValues.length) return 0;
        const index = sortedValues.indexOf(val);
        return index / (sortedValues.length - 1);
    }
};

/**
 * Matrix Data Access Layer (Selectors).
 * Normalizes raw store data for the Matrix component.
 */
export const matrixSelectors = {
    /**
     * Prepares data snapshot for the matrix renderer.
     * @param {Object} state - Current global state.
     * @param {Object} options - { mode: 'cumulative'|'expiry', strategy: 'linear'|'log'|'percentile' }
     */
    selectMatrixData: (state, options = { mode: 'cumulative', strategy: 'log' }) => {
        const { activeTicker, data } = state;
        const tickerData = data[activeTicker];

        if (!tickerData || !tickerData.chain || !tickerData.chain.length) {
            return null;
        }

        const mode = options.mode || 'cumulative';
        const strategy = options.strategy || 'log';

        // 1. Identify Strikes and Expiries
        const strikes = [...new Set(tickerData.chain.map(c => parseFloat(c.strike)))].sort((a, b) => b - a);
        const expiries = [...new Set(tickerData.chain.map(c => c.expiry))].sort();

        // 2. Aggregate / Filter Data based on Mode
        // For simplicity, if mode is cumulative, we sum across expiries (though usually matrix shows per-expiry)
        // If the API provides a pre-aggregated matrix, we use it. 
        // Here we build it from the chain.
        const cells = {};
        const gexValues = [];
        const dexValues = [];

        tickerData.chain.forEach(item => {
            const key = `${item.strike}:${item.expiry}`;
            const gex = parseFloat(item.gex || 0);
            const dex = parseFloat(item.dex || 0);

            cells[key] = {
                gex: { value: gex, sign: Math.sign(gex) },
                dex: { value: dex, sign: Math.sign(dex) }
            };

            if (gex > 0) gexValues.push(gex);
            if (Math.abs(dex) > 0) dexValues.push(Math.abs(dex));
        });

        // 3. Independent Normalization Ranges
        const gexMax = gexValues.length ? Math.max(...gexValues) : 0;
        const gexMin = 0;
        const dexMax = dexValues.length ? Math.max(...dexValues) : 0;
        const dexMin = 0;

        const sortedGex = [...gexValues].sort((a, b) => a - b);
        const sortedDex = [...dexValues].sort((a, b) => a - b);

        // 4. Calculate Intensities
        Object.keys(cells).forEach(key => {
            const cell = cells[key];

            // GEX Intensity
            if (strategy === 'percentile') {
                cell.gex.intensity = STRATEGIES.percentile(cell.gex.value, sortedGex);
            } else {
                cell.gex.intensity = STRATEGIES[strategy](cell.gex.value, gexMin, gexMax);
            }

            // DEX Intensity (on absolute value)
            if (strategy === 'percentile') {
                cell.dex.intensity = STRATEGIES.percentile(Math.abs(cell.dex.value), sortedDex);
            } else {
                cell.dex.intensity = STRATEGIES[strategy](Math.abs(cell.dex.value), dexMin, dexMax);
            }

            // Map intensity (0-1) to discrete levels (1-10)
            cell.gex.level = Math.ceil(cell.gex.intensity * 10);
            cell.dex.level = Math.ceil(cell.dex.intensity * 10);
        });

        return {
            ticker: activeTicker,
            mode: mode,
            spotPrice: tickerData.spotPrice || tickerData.price,
            strikes: strikes,
            expiries: expiries,
            cells: cells,
            overlays: {
                zeroGamma: tickerData.zeroGamma || tickerData.maxPain || null // Placeholder for Zero Gamma logic
            },
            stats: {
                gex: { max: gexMax, min: gexMin },
                dex: { max: dexMax, min: dexMin }
            }
        };
    }
};
