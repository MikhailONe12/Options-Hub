function randomSeries(n, base){
  const arr=[]; let v=base; for(let i=0;i<n;i++){ v += (Math.random()-0.5)*base*0.01; arr.push(Number(v.toFixed(2))); } return arr;
}
function ruDataSource(ticker){
  // Stub synthetic data until real adapter implemented
  const seed = ticker === 'GAZP' ? 250 : 150;
  return {
    spotHistory: randomSeries(120, seed),
    futuresHistory: randomSeries(120, seed*1.01),
    spotPrice: seed + (Math.random()-0.5)*2,
    futuresPrice: seed*1.01 + (Math.random()-0.5)*2
  };
}

export const russianPage = {
  id: 'russian',
  tab: 'russian',
  tickers: ['GAZP','SBER'],
  // Пока без блоков, график скрыт
  layout: []
};
