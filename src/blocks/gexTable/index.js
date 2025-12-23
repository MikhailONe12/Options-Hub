import { registerBlock } from '../../core/blockRegistry.js';
import { store } from '../../state/store.js';
import { render } from './render.js';

/**
 * GEX Table Block
 * Thin Block Contract v1.0
 * 
 * Acts as a controller/view for GEX expiration dates and modes.
 * Updates the global store on interaction.
 */
export const gexTableBlock = {
  id: 'gexTable',

  selectors: {
    data: 'selectGexTableData',
    meta: 'selectAssetMeta'
  },

  mount(container, ctx) {
    this.renderBlock(container, ctx);
  },

  update(container, ctx) {
    this.renderBlock(container, ctx);
  },

  renderBlock(container, ctx) {
    if (!ctx.data) {
      container.innerHTML = '<div class="table-empty">No Expirations Found</div>';
      return;
    }

    // 1. Render UI
    render(container, ctx);

    // 2. Bind Events (Directly to the container)
    this.bindEvents(container);
  },

  bindEvents(container) {
    // Mode Toggles
    container.querySelectorAll('.mode-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const mode = btn.dataset.mode;
        store.setState({ gexMode: mode }, 'gexTable:mode');
      });
    });

    // Expiry Selection
    container.querySelectorAll('.expiry-item').forEach(item => {
      item.addEventListener('click', () => {
        const expiry = item.dataset.expiry;
        store.setState({ selectedExpiry: expiry }, 'gexTable:expiry');
      });
    });
  },

  unmount(container) {
    container.innerHTML = '';
  }
};

registerBlock(gexTableBlock);
