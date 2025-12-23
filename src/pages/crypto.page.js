function cryptoDataSource(ticker){
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
    underlying_price: d.underlying_price ?? d.spotPrice ?? 0,
    // Enhanced volatility metrics for priceComparison block
    Sell_Score: d.Sell_Score ?? null,
    Buy_Score: d.Buy_Score ?? null,
    implied_volatility: d.implied_volatility ?? null,
    historical_volatility: d.historical_volatility ?? null,
    realized_volatility: d.realized_volatility ?? null,
    Gap: d.Gap ?? null,
    IV_pct: d.IV_pct ?? null,
    HV_pct: d.HV_pct ?? null,
    RV_pct: d.RV_pct ?? null
  };
}

export const cryptoPage = {
  id: 'crypto',
  tickers: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'MNTUSDT', 'DOGEUSDT'],
  dataSource: cryptoDataSource,
  grid: { columns: 12, rowHeight: 160 },
  layout: [
    { block: 'priceComparison', props: { width: 900, height: 300, startLabel: { fontSize: 13 } } },
    { block: 'greeks', col: 1, colSpan: 12, row: 3, rowSpan: 2, props: {} }
  ]
};