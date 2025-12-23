import { registerBlock } from '../../core/blockRegistry.js';
import { render } from './render.js';

/**
 * Universal Metric Block
 * Thin Block Contract v1.0
 */
export const metricBlock = {
    id: 'metric',

    selectors: {
        data: 'selectGexMetric', // Default selector
        meta: 'selectAssetMeta'
    },

    mount: render,
    update: render,

    unmount(container) {
        container.innerHTML = '';
    }
};

registerBlock(metricBlock);
