import { registerBlock } from '../../core/blockRegistry.js';
import { render } from './render.js';

/**
 * MaxPain Block
 * Thin Block Contract v1.0
 */
export const maxPainBlock = {
  id: 'maxPain',

  selectors: {
    data: 'selectMaxPainData',
    meta: 'selectAssetMeta'
  },

  mount: render,
  update: render,

  unmount(container) {
    container.innerHTML = '';
  }
};

registerBlock(maxPainBlock);
