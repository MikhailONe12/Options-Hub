import { registerBlock } from '../../core/blockRegistry.js';
import { render } from './render.js';

/**
 * GEX Matrix Block
 * Thin Block Contract v1.0 (Extended)
 * 
 * Handles the high-level orchestration of the matrix view,
 * including auto-scrolling to the current price.
 * 
 * Stateless Refactor:
 * - Removed this._container
 * - Removed this._isFirstRender (replaced with dataset check)
 */
export const gexMatrixBlock = {
  id: 'gexMatrix',

  selectors: {
    data: 'selectMatrixData',
    meta: 'selectAssetMeta'
  },

  /**
   * Mounts the block into the DOM.
   */
  mount(container, ctx) {
    this.renderBlock(container, ctx);
  },

  /**
   * Updates the block when the store or context changes.
   */
  update(container, ctx) {
    this.renderBlock(container, ctx);
  },

  /**
   * Internal render logic.
   */
  renderBlock(container, ctx) {
    if (!ctx || !ctx.data) {
      container.innerHTML = '<div class="matrix-empty">No Data Available</div>';
      return;
    }

    // 1. Delegate core rendering to the pure renderer
    render(container, ctx);

    // 2. Handle lifecycle-specific UX: auto-scroll to spot on first render
    // Use DOM state to track if we've scrolled (stateless singleton)
    if (!container.dataset.hasInitialScroll) {
      // Slight delay to ensure DOM is ready for scroll measurements
      requestAnimationFrame(() => {
        this.scrollToSpot(container, ctx.data.spotPrice);
        container.dataset.hasInitialScroll = "true";
      });
    }
  },

  /**
   * UX Helper: Centers the matrix view on the current spot price.
   */
  scrollToSpot(container, price) {
    const matrixBody = container.querySelector('.matrix-body');
    if (!matrixBody) return;

    // Find closest strike row
    const rows = Array.from(container.querySelectorAll('.matrix-row'));
    if (!rows.length) return;

    let closestRow = null;
    let minDiff = Infinity;

    rows.forEach(row => {
      const strike = parseFloat(row.dataset.strike);
      const diff = Math.abs(strike - price);
      if (diff < minDiff) {
        minDiff = diff;
        closestRow = row;
      }
    });

    if (closestRow) {
      const containerHeight = matrixBody.clientHeight;
      const rowOffsetTop = closestRow.offsetTop;
      const rowHeight = closestRow.offsetHeight;

      // Calculate center position
      const scrollPos = rowOffsetTop - (containerHeight / 2) + (rowHeight / 2);
      matrixBody.scrollTop = scrollPos;
    }
  },

  /**
   * Cleans up the block.
   */
  unmount(container) {
    container.innerHTML = '';
  }
};

registerBlock(gexMatrixBlock);
