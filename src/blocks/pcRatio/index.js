import { registerBlock } from '../../core/blockRegistry.js';
import { render } from './render.js';

/**
 * PC Ratio Block
 * Thin Block Contract v1.0
 */
export const pcRatioBlock = {
  id: 'pcRatio',

  selectors: {
    data: 'selectPcRatioData',
    meta: 'selectAssetMeta'
  },

  mount: render,
  update: render,

  unmount(container) {
    container.innerHTML = '';
  }
};

registerBlock(pcRatioBlock);
