import { registerBlock } from '../../core/blockRegistry.js';
import { render } from './render.js';

/**
 * Price Comparison Block
 * Thin Block Contract v1.0
 */
export const priceComparisonBlock = {
    id: 'priceComparison',

    selectors: {
        data: 'selectPriceComparisonData',
        meta: 'selectAssetMeta'
    },

    mount: render,
    update: render,

    unmount(container) {
        container.innerHTML = '';
    }
};

registerBlock(priceComparisonBlock);
