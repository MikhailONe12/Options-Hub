function btcDataSource(ticker){
  const d = (window.data && window.data[ticker]) || {};
  return {
    spotHistory: (Array.isArray(d.spotHistory) && d.spotHistory.length ? d.spotHistory : (Array.isArray(d.priceHistory) ? d.priceHistory : [])),
    futuresHistory: (Array.isArray(d.futuresHistory) ? d.futuresHistory : []),
    spotPrice: d.spotPrice ?? d.price,
    futuresPrice: d.futuresPrice ?? (d.futuresHistory||[]).slice(-1)[0],
    ivr: d.ivr ?? 0,
    callOi: d.callOi ?? 0,
    putOi: d.putOi ?? 0,
    // Greeks
    sum_Delta_OI: d.sum_Delta_OI ?? 0,
    sum_Gamma_OI: d.sum_Gamma_OI ?? 0,
    sum_Vega_OI: d.sum_Vega_OI ?? 0,
    sum_Theta_OI: d.sum_Theta_OI ?? 0,
    sum_Vanna_OI: d.sum_Vanna_OI ?? 0,
    sum_Charm_OI: d.sum_Charm_OI ?? 0,
    sum_Volga_OI: d.sum_Volga_OI ?? 0,
    sum_Veta_OI: d.sum_Veta_OI ?? 0,
    sum_Speed_OI: d.sum_Speed_OI ?? 0,
    sum_Zomma_OI: d.sum_Zomma_OI ?? 0,
    sum_Color_OI: d.sum_Color_OI ?? 0,
    sum_Ultima_OI: d.sum_Ultima_OI ?? 0,
    total_rho: d.total_rho ?? 0,
    // MaxPain
    maxPain: d.maxPain ?? 0,
    underlying_price: d.underlying_price ?? d.spotPrice ?? 0
  };
}

export const btcPage = {
  id: 'btc',
  tab: 'crypto',
  tickers: ['BTCUSDT'],
  dataSource: btcDataSource,
  grid: { columns: 12, rowHeight: 160 },
  layout: [
    { block: 'priceComparison', col: 1, colSpan: 12, row: 1, rowSpan: 2, props: { startLabel: { fontSize: 13 }, labelA: 'Spot', labelB: 'Futures', leftMain: 'BTC' } },
    { block: 'pcRatio', col: 1, colSpan: 3, row: 3, rowSpan: 2, props: { title: 'BTC P/C RATIO', callKey: 'callOi', putKey: 'putOi' } },
    { block: 'greeks', col: 4, colSpan: 5, row: 3, rowSpan: 2, props: {} },
    { block: 'maxPain', col: 9, colSpan: 4, row: 3, rowSpan: 2, props: {} },
    { block: 'gexTable', col: 1, colSpan: 12, row: 5, rowSpan: 2, props: {} },
    { block: 'gexMatrix', col: 1, colSpan: 12, row: 7, rowSpan: 6, props: {} },
    { block: 'volatilitySmile', col: 1, colSpan: 12, row: 13, rowSpan: 3, props: {} }
  ]
};