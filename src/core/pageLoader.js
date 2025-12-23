import { getBlock } from './blockRegistry.js';
import { selectors as selectorRegistry } from '../state/selectors.js';

/**
 * Resolves data for a block based on its explicit selectors and the current state.
 * Implements Thin Block Contract v1.0.
 */
function resolveData(blockDef, item, state) {
  const ticker = item.ticker || item.props?.ticker || state.activeTicker;
  const selectors = blockDef.selectors || {};

  const ctx = {
    ticker: ticker,
    data: null,
    meta: null,
    ui: {
      props: item.props || {},
      pageId: state.currentTab, // Using currentTab as a proxy for page context
      theme: state.theme || 'dark'
    }
  };

  // 1. Resolve Data Dependency
  if (selectors.data) {
    if (typeof selectorRegistry[selectors.data] === 'function') {
      ctx.data = selectorRegistry[selectors.data](state, ticker);
    } else {
      console.error(`[pageLoader] Selector not found: ${selectors.data}`);
    }
  }

  // 2. Resolve Meta Dependency
  if (selectors.meta) {
    if (typeof selectorRegistry[selectors.meta] === 'function') {
      ctx.meta = selectorRegistry[selectors.meta](state, ticker);
    }
  }

  return ctx;
}

export { getBlock };

export async function loadPage(pageConfig, root, state) {
  root.innerHTML = '';

  // Grid Setup
  if (pageConfig.grid) {
    root.classList.add('page-grid');
    const cols = pageConfig.grid.columns || 12;
    const rowHeight = pageConfig.grid.rowHeight || 300;
    root.style.setProperty('--page-grid-columns', cols);
    root.style.setProperty('--page-grid-row-height', rowHeight + 'px');
  } else {
    root.classList.remove('page-grid');
  }

  if (!Array.isArray(pageConfig.layout)) {
    console.error('[pageLoader] pageConfig.layout is not an array:', pageConfig.layout);
    return;
  }
  pageConfig.layout.forEach(item => {
    // Подробное логирование для отладки
    console.log('[pageLoader] Processing layout item:', item);
    const blockId = item.id;
    const blockDef = getBlock(blockId);
    if (!blockDef) {
      console.error('[pageLoader] Block not found:', blockId, 'Full item:', item);
      return;
    }

    const container = document.createElement('div');
    container.className = 'mod-block-container';

    // Grid Positioning
    if (pageConfig.grid) {
      container.style.gridColumn = `${item.col || 1} / span ${item.colSpan || 1}`;
      container.style.gridRow = `${item.row || 1} / span ${item.rowSpan || 1}`;
    }

    root.appendChild(container);

    // Resolve context using Thin Block Contract
    const ctx = resolveData(blockDef, item, state);

    console.log(`[pageLoader] Mounting block: ${item.id}`, ctx);
    if (typeof blockDef.mount === 'function') {
      blockDef.mount(container, ctx);
    }
  });
}

export function updatePage(pageConfig, root, state) {
  const items = pageConfig.layout;
  const containers = Array.from(root.querySelectorAll('.mod-block-container'));

  containers.forEach((container, idx) => {
    const item = items[idx];
    const blockDef = getBlock(item.id);

    if (!blockDef || typeof blockDef.update !== 'function') return;

    // Resolve fresh context
    const ctx = resolveData(blockDef, item, state);

    // Update block
    blockDef.update(container, ctx);
  });
}
