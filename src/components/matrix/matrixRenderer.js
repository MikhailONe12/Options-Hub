import { store } from '../../state/store.js';
import { matrixSelectors } from './matrixSelectors.js';

/**
 * GEX / DEX Matrix Renderer.
 * Contract: Thin renderer, only DOM + attributes.
 */
export const matrixRenderer = {
    container: null,
    currentMode: 'cumulative',

    /**
     * Initializes the component.
     * @param {string} containerId - Anchor ID for the fragment.
     */
    init: (containerId) => {
        matrixRenderer.container = document.getElementById(containerId);
        if (!matrixRenderer.container) return;

        // Set up Mode Switches
        matrixRenderer.container.querySelectorAll('.mode-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const mode = btn.dataset.mode;
                matrixRenderer.currentMode = mode;

                // Update UI state
                matrixRenderer.container.querySelectorAll('.mode-btn').forEach(b =>
                    b.classList.toggle('active', b.dataset.mode === mode)
                );

                // Trigger immediate re-render with new mode
                matrixRenderer.refresh();
            });
        });

        // Subscribe to store updates
        store.subscribe(() => {
            matrixRenderer.refresh();
        });
    },

    /**
     * Triggers a render cycle using selectors.
     */
    refresh: () => {
        const state = store.getStateInternal();
        const matrixData = matrixSelectors.selectMatrixData(state, { mode: matrixRenderer.currentMode });

        if (matrixData) {
            matrixRenderer.render(matrixData);
        } else {
            matrixRenderer.clear();
        }
    },

    /**
     * Full render of the matrix view.
     */
    render: (data) => {
        if (!matrixRenderer.container) return;

        // 1. Update Ticker / Stats
        const tickerBadge = matrixRenderer.container.querySelector('[data-role="active-ticker"]');
        if (tickerBadge) tickerBadge.textContent = data.ticker;

        // 2. Render Grid Headers (Expiries)
        const header = matrixRenderer.container.querySelector('[data-role="matrix-expiries-header"]');
        if (header) {
            header.style.gridTemplateColumns = `repeat(${data.expiries.length}, 1fr)`;
            const fragment = document.createDocumentFragment();
            data.expiries.forEach(exp => {
                const div = document.createElement('div');
                div.className = 'expiry-label';
                div.textContent = exp;
                fragment.appendChild(div);
            });
            header.innerHTML = '';
            header.appendChild(fragment);
        }

        // 3. Render Grid Columns (Strikes)
        const sidebar = matrixRenderer.container.querySelector('[data-role="matrix-strikes-sidebar"]');
        if (sidebar) {
            const fragment = document.createDocumentFragment();
            data.strikes.forEach(strike => {
                const div = document.createElement('div');
                div.className = 'strike-label';
                // Highlight if near spot
                if (Math.abs(strike - data.spotPrice) < (data.spotPrice * 0.005)) {
                    div.classList.add('active');
                }
                div.textContent = strike.toLocaleString();
                fragment.appendChild(div);
            });
            sidebar.innerHTML = '';
            sidebar.appendChild(fragment);
        }

        // 4. Render Heatmap Grid
        const grid = matrixRenderer.container.querySelector('[data-role="matrix-grid"]');
        if (grid) {
            grid.style.gridTemplateColumns = `repeat(${data.expiries.length}, 1fr)`;
            const fragment = document.createDocumentFragment();

            // Grid is row-major (Strikes) then column-minor (Expiries)
            data.strikes.forEach(strike => {
                data.expiries.forEach(expiry => {
                    const cellKey = `${strike}:${expiry}`;
                    const cellData = data.cells[cellKey];

                    const dot = document.createElement('div');
                    dot.className = 'matrix-dot';

                    if (cellData) {
                        if (cellData.gex.level > 0) dot.setAttribute('data-gex-level', cellData.gex.level);
                        if (cellData.dex.level > 0) dot.setAttribute('data-dex-level', cellData.dex.level);

                        // Tooltip (simple version)
                        dot.title = `Strike: ${strike}\nExpiry: ${expiry}\nGEX: ${cellData.gex.value.toFixed(2)}\nDEX: ${cellData.dex.value.toFixed(2)}`;
                    }

                    // Zero Gamma Overlay
                    if (data.overlays.zeroGamma && Math.abs(strike - data.overlays.zeroGamma) < 1) {
                        dot.classList.add('zero-gamma-marker');
                    }

                    fragment.appendChild(dot);
                });
            });
            grid.innerHTML = '';
            grid.appendChild(fragment);
        }

        // 5. Update Statistics Panel
        matrixRenderer.updateStats(data);
    },

    /**
     * Updates the statistics column.
     */
    updateStats: (data) => {
        const setVal = (role, val) => {
            const el = matrixRenderer.container.querySelector(`[data-role="${role}"]`);
            if (el) el.textContent = val;
        };

        setVal('stat-zero-gamma', data.overlays.zeroGamma ? data.overlays.zeroGamma.toLocaleString() : '-');

        // Find Max GEX/DEX strikes from cells for the display
        let maxGexVal = -Infinity;
        let maxGexStrike = '-';
        let maxDexVal = -Infinity;
        let maxDexStrike = '-';

        let totalCallGex = 0;
        let totalPutGex = 0;

        Object.keys(data.cells).forEach(key => {
            const cell = data.cells[key];
            const strike = key.split(':')[0];

            if (cell.gex.value > maxGexVal) {
                maxGexVal = cell.gex.value;
                maxGexStrike = strike;
            }
            if (Math.abs(cell.dex.value) > maxDexVal) {
                maxDexVal = Math.abs(cell.dex.value);
                maxDexStrike = strike;
            }

            if (cell.gex.value > 0) totalCallGex += cell.gex.value;
            else totalPutGex += cell.gex.value;
        });

        setVal('stat-max-gex-strike', maxGexStrike);
        setVal('stat-max-dex-strike', maxDexStrike);
        setVal('stat-total-call-gex', totalCallGex.toLocaleString(undefined, { maximumFractionDigits: 0 }));
        setVal('stat-total-put-gex', totalPutGex.toLocaleString(undefined, { maximumFractionDigits: 0 }));
    },

    /**
     * Clears the matrix view.
     */
    clear: () => {
        if (!matrixRenderer.container) return;
        const grid = matrixRenderer.container.querySelector('[data-role="matrix-grid"]');
        if (grid) grid.innerHTML = '<div class="placeholder">Awaiting Data...</div>';
    }
};
