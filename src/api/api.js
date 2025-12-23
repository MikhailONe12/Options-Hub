const SERVER_BASE_KEY = 'optionsHub.serverBase';
let API_BASE = localStorage.getItem(SERVER_BASE_KEY) || '';

/**
 * Probes a base URL to see if it's a valid API.
 * @param {string} base - The base URL to probe.
 * @returns {Promise<boolean>}
 */
async function probeApi(base) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 5000);
    try {
        const r = await fetch(`${base}/api/test-gex`, { signal: ctrl.signal, credentials: 'omit' });
        clearTimeout(t);
        return !!(r && r.ok);
    } catch (_) {
        clearTimeout(t);
        return false;
    }
}

/**
 * Detects the API base URL by scanning common ports and origins.
 * @returns {Promise<string>} - The detected API base URL.
 */
export async function detectApiBase() {
    if (API_BASE && await probeApi(API_BASE)) return API_BASE;

    const host = window.location.hostname || 'localhost';
    const protocol = window.location.protocol || 'http:';
    const candidates = [window.location.origin];

    // Try common ports
    [9004, 9001, 9000].forEach(p => candidates.push(`${protocol}//${host}:${p}`));

    // Neighbor ports to UI
    const uiPort = parseInt(window.location.port || '0', 10);
    if (uiPort) {
        [uiPort - 1, uiPort + 1].forEach(p => { if (p > 0) candidates.push(`${protocol}//${host}:${p}`); });
    }

    // Wider scan window
    for (let p = 8995; p <= 9010; p++) candidates.push(`${protocol}//${host}:${p}`);

    // De-duplicate and probe
    const seen = new Set();
    const uniq = candidates.filter(c => { if (seen.has(c)) return false; seen.add(c); return true; });

    for (const base of uniq) {
        if (await probeApi(base)) {
            API_BASE = base;
            localStorage.setItem(SERVER_BASE_KEY, API_BASE);
            return API_BASE;
        }
    }
    return '';
}

/**
 * Internal helper for timed fetch.
 */
const createTimedFetch = (url, timeout = 60000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);
    return fetch(url, { signal: controller.signal, credentials: 'omit' })
        .finally(() => clearTimeout(timeoutId));
};

/**
 * API Data Access Object.
 * Contract: Pure fetching, returns raw data, NO store mutation.
 */
export const apiClient = {
    /**
     * Gets the current API Base URL.
     */
    getBaseUrl: () => API_BASE,

    /**
     * Fetches market snapshots for all supported symbols.
     * @returns {Promise<Object>} - Map of symbols to { latest, history } JSON data.
     */
    fetchAllMarketData: async () => {
        if (!API_BASE) await detectApiBase();
        if (!API_BASE) throw new Error('[API] Base URL not detected');

        const symbols = ['btc', 'eth', 'sol', 'xrp', 'mnt', 'doge'];
        const ruSymbols = ['imoex', 'usdrub'];

        // Prepare list of promises
        const requests = [];

        // Crypto
        symbols.forEach(s => {
            requests.push(createTimedFetch(`${API_BASE}/api/${s}-metrics/latest`));
            requests.push(createTimedFetch(`${API_BASE}/api/${s}-metrics/latest/288`));
        });

        // RU Market (Real + Mock)
        ruSymbols.forEach(s => {
            if (s === 'usdrub') {
                // Mock USDRUB for now
                requests.push(Promise.resolve({
                    ok: true,
                    json: () => Promise.resolve({
                        // Mock Latest
                        price: 92.45 + (Math.random() - 0.5),
                        futuresPrice: 93.10 + (Math.random() - 0.5),
                        iv: 0.15 + (Math.random() * 0.01),
                        hv: 0.12,
                        Sell_Score: 0.45,
                        Buy_Score: 0.65
                    })
                }));
                requests.push(Promise.resolve({
                    ok: true,
                    json: () => Promise.resolve(
                        // Mock History (50 points)
                        Array.from({ length: 50 }, (_, i) => ({
                            price: 90 + (i * 0.05) + Math.random(),
                            futuresPrice: 90.5 + (i * 0.05) + Math.random(),
                            timestamp: Date.now() - (50 - i) * 60000
                        }))
                    )
                }));
            } else {
                // IMOEX Real Fetch
                requests.push(createTimedFetch(`${API_BASE}/api/${s}-metrics/latest`));
                requests.push(createTimedFetch(`${API_BASE}/api/${s}-metrics/latest/288`));
            }
        });

        // Fetch everything in parallel
        const results = await Promise.allSettled(requests);

        const dataMap = {};
        const allSymbols = [...symbols, ...ruSymbols];

        for (let i = 0; i < allSymbols.length; i++) {
            const symbol = allSymbols[i];
            const symbolKey = symbol.toUpperCase() + (['imoex', 'usdrub'].includes(symbol) ? '' : 'USDT');

            const latestRes = results[i * 2];
            const historyRes = results[i * 2 + 1];

            let latestJson = null;
            let historyJson = null;

            if (latestRes.status === 'fulfilled' && latestRes.value.ok) {
                try { latestJson = await latestRes.value.json(); } catch (_) { }
            }
            if (historyRes.status === 'fulfilled' && historyRes.value.ok) {
                try { historyJson = await historyRes.value.json(); } catch (_) { }
            }

            dataMap[symbolKey] = {
                latest: latestJson,
                history: historyJson,
                // Helper mapping for flat props
                ...latestJson,
                spotHistory: historyJson?.map(h => h.price) || [],
                futuresHistory: historyJson?.map(h => h.futuresPrice || h.price) || [] // Fallback
            };
        }

        return dataMap;
    }
};
