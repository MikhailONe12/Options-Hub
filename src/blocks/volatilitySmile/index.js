import { registerBlock } from '../../core/blockRegistry.js';
import { render } from './render.js';

/**
 * Volatility Smile Block
 * Thin Block Contract v1.0
 */
export const volatilitySmileBlock = {
  id: 'volatilitySmile',

  selectors: {
    data: 'selectSmileData',
    meta: 'selectAssetMeta'
  },

  mount: render,
  update: render,

  unmount(container) {
    container.innerHTML = '';
  }
};

registerBlock(volatilitySmileBlock);