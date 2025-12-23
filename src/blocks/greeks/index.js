import { registerBlock } from '../../core/blockRegistry.js';
import { render } from './render.js';

/**
 * Greeks Block
 * Thin Block Contract v1.0
 */
export const greeksBlock = {
  id: 'greeks',

  selectors: {
    data: 'selectGreekData',
    meta: 'selectAssetMeta'
  },

  mount(container, ctx) {
    console.log('[greeksBlock] Mount - ctx:', ctx);
    console.log('[greeksBlock] Mount - ctx.data:', ctx.data);
    render(container, ctx);
  },
  
  update(container, ctx) {
    console.log('[greeksBlock] Update - ctx:', ctx);
    console.log('[greeksBlock] Update - ctx.data:', ctx.data);
    render(container, ctx);
  },

  unmount(container) {
    container.innerHTML = '';
  }
};

registerBlock(greeksBlock);
