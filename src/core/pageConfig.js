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
        layout: [
            { id: 'greeks', ticker: 'BTCUSDT' },
            { id: 'maxPain', ticker: 'BTCUSDT' },
            { id: 'pcRatio', ticker: 'BTCUSDT' },
            { id: 'priceComparison', ticker: 'BTCUSDT' },
            { id: 'volatilitySmile', ticker: 'BTCUSDT' },
            { id: 'gexTable', ticker: 'BTCUSDT' },
            { id: 'gexMatrix', ticker: 'BTCUSDT' }
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
