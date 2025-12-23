import { fragmentLoader } from './core/fragmentLoader.js';
import './blocks/index.js'; // Register all blocks
import { store } from './state/store.js';
import { apiClient } from './api/api.js';
import { sidebarRenderer } from './components/sidebar/sidebarRenderer.js';
import { headerRenderer } from './components/header/headerRenderer.js';
import { pageRenderer } from './core/pageRenderer.js';
import { pages } from './core/pageConfig.js';
import { calculatePriceChange } from './domain/metrics/priceChange.js';

/**
 * Main Application Bootstrap Layer.
 * Orchestrates fragment loading, component initialization, and the data lifecycle.
 */
const App = {
        /**
         * Starts polling for data updates (alias for startUpdateLoop).
         */
        startPolling: () => {
            App.startUpdateLoop();
        },
    /**
     * Entry Point: Initialize the modular application.
     */
    init: async () => {
        console.log('[App] Initializing modular frontend with Page System...');

        try {
            // 1. Load HTML Fragments (Common Layout)
            console.log('[App] Loading fragments...');
            await Promise.all([
                fragmentLoader.load('ticker-sidebar-container', './src/components/sidebar/sidebar.html'),
                fragmentLoader.load('header-container', './src/components/header/header.html')
            ]);
            console.log('[App] Fragments loaded.');

            // 2. Initialize Core Components
            console.log('[App] Initializing sidebar and header...');
            sidebarRenderer.init();
            headerRenderer.init();

            // 3. Initialize Page Renderer
            const matrixRoot = document.getElementById('matrix-root');
            if (matrixRoot) {
                console.log('[App] Initializing PageRenderer on #matrix-root');
                pageRenderer.initPage(pages.crypto, matrixRoot);
            } else {
                console.warn('[App] #matrix-root not found, skipping PageRenderer init.');
            }

            // 4. Initial Data Load
            console.log('[App] Starting initial data refresh...');
            await App.refreshData();
            console.log('[App] Initial data refreshed.');

            // 5. Start Polling
            App.startPolling();

            // 6. Bind Global UI Events (Tabs)
            App.bindUIEvents();

            // Signal Ready
            window.dispatchEvent(new CustomEvent('data:ready'));
            console.log('[App] Initialization complete. Event "data:ready" fired.');
        } catch (e) {
            console.error('[App] Init failed:', e);
            throw e; // Propagate to main.js catcher
        }
    },

    /**
     * Binds global events likes tab switching.
     */
    bindUIEvents: () => {
        const tabs = document.querySelectorAll('.nav-tab');
        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                const tabId = tab.dataset.tab;
                if (tabId) {
                    store.setState({ currentTab: tabId }, 'app:tab-switch');

                    // Update active class (visual only, pageRenderer handles logic)
                    tabs.forEach(t => t.classList.remove('active'));
                    tab.classList.add('active');
                }
            });
        });
    },

    /**
     * Refreshes market data and updates the store.
     */
    refreshData: async () => {
        try {
            const snapshots = await apiClient.fetchAllMarketData();
            const currentState = store.getStateInternal();
            const newData = { ...currentState.data };

            for (const [symbolKey, payload] of Object.entries(snapshots)) {
                if (payload.latest) {
                    // Normalize symbol: 'BTCUSDT' -> 'BTC', 'IMOEX' -> 'IMOEX'
                    const normalizedKey = symbolKey.replace('USDT', '').toUpperCase();
                    console.log('[App.refreshData] Storing data with key:', normalizedKey, 'from API key:', symbolKey);
                    newData[normalizedKey] = App.transformApiData(symbolKey, payload.latest, payload.history, currentState.data[normalizedKey]);
                    // If payload has a chain, update it too
                    if (payload.chain) {
                        newData[normalizedKey].chain = payload.chain;
                    }
                }
            }

            console.log('[App.refreshData] Final data keys:', Object.keys(newData));
            store.setState({ data: newData });

            const lastUpdateEl = document.getElementById('last-update');
            if (lastUpdateEl) {
                lastUpdateEl.textContent = `Last update: ${new Date().toLocaleTimeString()}`;
            }

            window.dispatchEvent(new CustomEvent('data:update'));
        } catch (error) {
            console.error('[App] Data refresh cycle failed:', error);
        }
    },

    /**
     * Maps raw API data to state structure.
     */
    transformApiData: (symbol, latest, history, prevData) => {
        const spotPrice = parseFloat(latest.underlying_price || 0);

        const priceHistory = Array.isArray(history)
            ? history.map(h => parseFloat(h.underlying_price || 0)).filter(p => p > 0)
            : [];

        const changePct = calculatePriceChange(priceHistory);

        console.debug('[App.transformApiData]', {
            symbol,
            spotPrice,
            priceHistoryPoints: priceHistory.length,
            changePct
        });

        return {
            ...prevData,
            price: spotPrice,
            spotPrice: spotPrice,
            underlying_price: spotPrice,
            changePct: changePct,
            gex: parseFloat(latest.raw_gex || latest.gex || 0),
            dex: parseFloat(latest.dex || latest.raw_dex || 0),
            iv: parseFloat(latest.implied_volatility || 0) * 100,
            ivr: parseFloat(latest.ivr || 0),
            callOi: parseFloat(latest.callOi || 0),
            putOi: parseFloat(latest.putOi || 0),
            sum_Delta_OI: parseFloat(latest.sum_Delta_OI || 0),
            sum_Gamma_OI: parseFloat(latest.sum_Gamma_OI || 0),
            sum_Vega_OI: parseFloat(latest.sum_Vega_OI || 0),
            sum_Theta_OI: parseFloat(latest.sum_Theta_OI || 0),
            sum_Vanna_OI: parseFloat(latest.sum_Vanna_OI || 0),
            sum_Charm_OI: parseFloat(latest.sum_Charm_OI || 0),
            sum_Volga_OI: parseFloat(latest.sum_Volga_OI || 0),
            sum_Veta_OI: parseFloat(latest.sum_Veta_OI || 0),
            sum_Speed_OI: parseFloat(latest.sum_Speed_OI || 0),
            sum_Zomma_OI: parseFloat(latest.sum_Zomma_OI || 0),
            sum_Color_OI: parseFloat(latest.sum_Color_OI || 0),
            sum_Ultima_OI: parseFloat(latest.sum_Ultima_OI || 0),
            total_rho: parseFloat(latest.total_rho || 0),
            maxPain: parseFloat(latest.max_pain || 0),
            futuresPrice: parseFloat(latest.futures_price || 0),
            priceHistory: priceHistory,
        };
    },

    /**
     * Timer for periodic refresh.
     */
    startUpdateLoop: () => {
        const INTERVAL = 30000;
        const latencyEl = document.getElementById('latency');
        let countdown = INTERVAL / 1000;

        setInterval(async () => {
            await App.refreshData();
            countdown = INTERVAL / 1000;
        }, INTERVAL);

        setInterval(() => {
            countdown--;
            if (latencyEl) {
                latencyEl.textContent = `${countdown} sec`;
            }
            if (countdown <= 0) countdown = INTERVAL / 1000;
        }, 1000);
    }
};

export default App;
