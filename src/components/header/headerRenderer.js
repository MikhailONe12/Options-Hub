import { store } from '../../state/store.js';
import { selectors } from '../../state/selectors.js';

/**
 * Header Component Renderer.
 * Handles top-level navigation tabs and exchange mini-tabs.
 */
export const headerRenderer = {
    /**
     * Initializes the header component.
     */
    init: () => {
        // Main Tabs
        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', () => {
                headerRenderer.handleTabClick(tab.dataset.tab);
            });
        });

        // Exchange Tabs (Crypto & RU)
        document.querySelectorAll('.exchange-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                if (tab.dataset.exchange) {
                    headerRenderer.handleExchangeClick(tab.dataset.exchange);
                } else if (tab.dataset.ruSubtab) {
                    headerRenderer.handleRuSubtabClick(tab.dataset.ruSubtab);
                }
            });
        });

        // Subscribe to store updates
        store.subscribe(() => {
            headerRenderer.render();
        });

        // Initial render
        headerRenderer.render();
    },

    /**
     * Renders header states, including active tabs and layout visibility.
     */
    render: () => {
        const state = selectors.getState();
        const currentTab = selectors.getCurrentTab(state);
        const currentExchange = selectors.getCurrentExchange(state);

        // Update Main Tabs active state
        document.querySelectorAll('.tab').forEach(tab => {
            tab.classList.toggle('active', tab.dataset.tab === currentTab);
        });

        // Update Exchange Tabs active state
        document.querySelectorAll('.exchange-tab').forEach(tab => {
            if (tab.dataset.exchange) {
                tab.classList.toggle('active', tab.dataset.exchange === currentExchange);
            }
        });

        // Sync visibility of containers
        const cryptoExContainer = document.getElementById('exchange-tabs-container');
        const ruExContainer = document.getElementById('ru-tabs-container');
        const main = document.querySelector('main');

        if (cryptoExContainer && ruExContainer && main) {
            const isCrypto = currentTab === 'crypto';
            const isRu = currentTab === 'ru';

            cryptoExContainer.classList.toggle('active', isCrypto);
            ruExContainer.classList.toggle('active', isRu);
            main.classList.toggle('exchange-tabs-visible', isCrypto || isRu);
        }
    },

    /**
     * Handles main navigation tab clicks.
     */
    handleTabClick: (tabId) => {
        store.setState({ currentTab: tabId });

        // Toggle tab content visibility
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.toggle('active', content.id === `${tabId}-content`);
        });

        // Emit legacy event for compatibility
        try { window.dispatchEvent(new CustomEvent('context:tab', { detail: { tab: tabId } })); } catch (_) { }
    },

    /**
     * Handles exchange mini-tab clicks.
     */
    handleExchangeClick: (exchangeId) => {
        store.setState({ currentExchange: exchangeId });

        // Placeholder logic for OKX/BingX (mirrors legacy index.html)
        const cryptoContent = document.getElementById('crypto-content');
        if (cryptoContent) {
            if (exchangeId === 'okx' || exchangeId === 'bingx') {
                cryptoContent.innerHTML = `
                    <div style="display: flex; justify-content: center; align-items: center; height: 60vh;">
                        <div style="text-align: center; padding: 20px; background: var(--bg-card); border: 1px solid var(--border); border-radius: 4px;">
                            <h2 style="color: var(--text-primary); margin-bottom: 10px;">${exchangeId.toUpperCase()} Exchange</h2>
                            <p style="color: var(--text-secondary); font-size: 16px;">This exchange is currently under development</p>
                        </div>
                    </div>
                `;
            } else {
                // Restore app container for Bybit
                cryptoContent.innerHTML = '<div id="app"></div>';
                // Trigger re-render of components if needed (app.js will handle initialization)
            }
        }

        // If switching TO Bybit, ensure we have an active ticker
        if (exchangeId === 'bybit') {
            store.setState({ activeTicker: 'BTCUSDT' });
        }
    },

    /**
     * Handles RU Subtab clicks.
     */
    handleRuSubtabClick: (subtabId) => {
        document.querySelectorAll('.ru-sub-content').forEach(content => {
            content.style.display = content.id === `${subtabId}-content` ? 'block' : 'none';
        });

        document.querySelectorAll('[data-ru-subtab]').forEach(tab => {
            tab.classList.toggle('active', tab.dataset.ruSubtab === subtabId);
        });
    }
};
