import { store } from '../state/store.js';
import * as pageLoader from './pageLoader.js';

/**
 * pageRenderer - Orchestrates the lifecycle of block-based pages.
 * It connects the pageLoader to the centralized store.
 */
export const pageRenderer = {
    _activePageConfig: null,
    _activeRoot: null,
    _unsubscribe: null,

    /**
     * Initializes a page in a specific root element.
     * @param {Object} pageConfig - Configuration for the page (layout, grid, filters).
     * @param {HTMLElement} root - The DOM element to render the page into.
     */
    initPage(pageConfig, root) {
        console.log(`[pageRenderer] Initializing page: ${pageConfig.id}`);

        // 1. Cleanup previous page if exists
        this.cleanup();

        this._activePageConfig = pageConfig;
        this._activeRoot = root;

        // 2. Initial Mount
        const state = store.getStateInternal();
        pageLoader.loadPage(this._activePageConfig, this._activeRoot, state);

        // 3. Subscribe to Store Updates
        this._unsubscribe = store.subscribe((state) => {
            console.log(`[pageRenderer] Store updated, updating page: ${this._activePageConfig.id}`);
            pageLoader.updatePage(this._activePageConfig, this._activeRoot, state);
        });
    },

    /**
     * Cleans up the current page and unsubscribes from store updates.
     */
    cleanup() {
        if (this._unsubscribe) {
            console.log(`[pageRenderer] Cleaning up page: ${this._activePageConfig?.id}`);
            this._unsubscribe();
            this._unsubscribe = null;
        }

        if (this._activeRoot && this._activePageConfig) {
            // Call unmount for each block if it has one
            const blocks = Array.from(this._activeRoot.querySelectorAll('.mod-block-container'));
            blocks.forEach((container, idx) => {
                const item = this._activePageConfig.layout[idx];
                const blockDef = pageLoader.getBlock(item.block);
                if (blockDef && typeof blockDef.unmount === 'function') {
                    blockDef.unmount(container);
                }
            });
            this._activeRoot.innerHTML = '';
        }

        this._activePageConfig = null;
        this._activeRoot = null;
    }
};
