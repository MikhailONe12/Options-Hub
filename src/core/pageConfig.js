/**
 * Page Configurations
 * Defines the layout and blocks for each page/tab.
 *
 * layout: Array<BlockDescriptor>
 * Order defines render order.
 * Zoning handled by layout engine / CSS.
 */
export const pages = {
    crypto: {
        id: 'crypto',
        title: 'Crypto Market',
        grid: { columns: 12, rowHeight: 320 },
        layout: [
            // Row 1: Price Comparison (Full Width)
            { id: 'priceComparison', ticker: 'BTCUSDT', col: 1, colSpan: 12, row: 1, rowSpan: 1 },

            // Row 2: PC Ratio (3), Greeks (5), Max Pain (4)
            { id: 'pcRatio', ticker: 'BTCUSDT', col: 1, colSpan: 3, row: 2, rowSpan: 1 },
            { id: 'greeks', ticker: 'BTCUSDT', col: 4, colSpan: 5, row: 2, rowSpan: 1 },
            { id: 'maxPain', ticker: 'BTCUSDT', col: 9, colSpan: 4, row: 2, rowSpan: 1 },

            // Row 3: GeX Table (Full Width)
            { id: 'gexTable', ticker: 'BTCUSDT', col: 1, colSpan: 12, row: 3, rowSpan: 1 },

            // Row 4: GeX Matrix (Full Width)
            { id: 'gexMatrix', ticker: 'BTCUSDT', col: 1, colSpan: 12, row: 4, rowSpan: 2 }
        ]
    },
    'ru-market': {
        id: 'ru-market',
        title: 'RU Market Indicators',
        layout: [
            { id: 'metric', props: { label: 'USDRUB', selector: 'selectUsdRubMetric' } },
            { id: 'metric', props: { label: 'IMOEX', selector: 'selectImoexMetric' } },
            { id: 'priceComparison', ticker: 'IMOEX' },
            { id: 'priceComparison', ticker: 'USDRUB' },
            { id: 'volatilitySmile', ticker: 'IMOEX' },
            { id: 'gexTable', ticker: 'IMOEX' },
            { id: 'gexMatrix', ticker: 'IMOEX' }
        ]
    }
};
