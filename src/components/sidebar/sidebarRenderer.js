import { store } from '../../state/store.js';
import { selectors } from '../../state/selectors.js';

/**
 * Sidebar Component Renderer.
 * Handles watchlist rendering, item selection, and search.
 */
export const sidebarRenderer = {
    /**
     * Initializes the sidebar component.
     * Hooks up event listeners and performing initial render.
     */
    init: () => {
        const searchInput = document.getElementById('ticker-search-input');
        if (searchInput) {
            searchInput.addEventListener('input', () => {
                sidebarRenderer.render();
            });
        }

        const collapseBtn = document.getElementById('collapse-sidebar');
        if (collapseBtn) {
            collapseBtn.addEventListener('click', () => {
                const sidebar = document.getElementById('ticker-sidebar');
                if (sidebar) {
                    sidebar.classList.toggle('collapsed');
                    collapseBtn.textContent = sidebar.classList.contains('collapsed') ? '›' : '‹';
                }
            });
        }

        // Subscribe to store updates
        store.subscribe(() => {
            sidebarRenderer.render();
        });

        // Initial render
        sidebarRenderer.render();
    },

    /**
     * Renders the watchlist based on current state.
     */
    render: () => {
        const state = selectors.getState();
        const exchange = selectors.getCurrentExchange(state);
        const assets = selectors.getExchangeAssets(state, exchange);
        const activeTicker = selectors.getActiveTicker(state);
        const tickerList = document.getElementById('ticker-list');
        const searchInput = document.getElementById('ticker-search-input');
        const searchTerm = searchInput ? searchInput.value.toLowerCase() : '';

        console.log('[sidebar] Rendering watchlist, exchange:', exchange, 'assets:', assets);
        console.log('[sidebar] state.data:', state.data);

        if (!tickerList) return;

        // Filter assets by search term
        const filteredAssets = assets.filter(asset =>
            asset.symbol.toLowerCase().includes(searchTerm) ||
            asset.name.toLowerCase().includes(searchTerm)
        );

        // We use a DocumentFragment for performance if the list is long
        const fragment = document.createDocumentFragment();

        filteredAssets.forEach((asset, idx) => {
            const changePctValue = parseFloat(asset.changePct || 0);
            const changeClass = changePctValue >= 0 ? 'positive' : 'negative';
            const changeSign = changePctValue > 0 ? '+' : (changePctValue < 0 ? '-' : '');

            // Normalize symbol for active state check (removing / or -)
            const normalizedSymbol = asset.symbol.toUpperCase().replace(/[-\/]/g, '');
            const isActive = normalizedSymbol === activeTicker;

            const item = document.createElement('div');
            item.className = `ticker-item ${isActive ? 'active' : ''}`;
            item.dataset.symbol = asset.symbol;
            item.dataset.exchange = exchange;
            item.style.animationDelay = `${idx * 0.05}s`;

            console.log(`[sidebar] Asset ${asset.symbol}: price=${asset.price}, changePct=${changePctValue}%`);

            item.innerHTML = `
                <div class="ticker-symbol">${asset.symbol}</div>
                <div class="ticker-name">${asset.name}</div>
                <div class="ticker-price-info">
                    <span class="ticker-price">${asset.price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                    <span class="ticker-change ${changeClass}">${changeSign}${Math.abs(changePctValue).toFixed(2)}%</span>
                </div>
            `;

            item.addEventListener('click', () => {
                sidebarRenderer.handleAssetClick(asset.symbol, exchange);
            });

            fragment.appendChild(item);
        });

        tickerList.innerHTML = '';
        tickerList.appendChild(fragment);
    },

    /**
     * Handles asset selection.
     */
    handleAssetClick: (symbol, exchange) => {
        const normalizedTicker = symbol.toUpperCase().replace(/[-\/]/g, '');

        // Update global state
        store.setState({
            activeTicker: normalizedTicker,
            currentExchange: exchange
        });

        // Dispatch compatibility events for external/legacy parts of the app
        try {
            window.dispatchEvent(new CustomEvent('context:exchange', { detail: { exchange } }));
            window.dispatchEvent(new CustomEvent('context:ticker', { detail: { ticker: normalizedTicker } }));
        } catch (_) { }
    }
};
