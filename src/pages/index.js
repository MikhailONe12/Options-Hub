import { btcPage } from './btc.page.js';
import { ethPage } from './eth.page.js';
import { solPage } from './sol.page.js';
import { xrpPage } from './xrp.page.js';
import { mntPage } from './mnt.page.js';
import { dogePage } from './doge.page.js';
import { russianPage } from './russian.page.js';
import { usPage } from './us.page.js';
import { euPage } from './eu.page.js';
import { cryptoPage } from './crypto.page.js';

const pages = [btcPage, ethPage, solPage, xrpPage, mntPage, dogePage, russianPage, usPage, euPage, cryptoPage];

export function getPageForContext(tab, ticker){
  const t = ticker ? ticker.toUpperCase() : null;
  // Сначала ищем страницу строго по табу и тикеру
  let match = pages.find(p => p.tab === tab && (!t || p.tickers.some(x => x.toUpperCase() === t)));
  if(match) return match;
  // Затем fallback: страница по табу без учёта тикера
  match = pages.find(p => p.tab === tab);
  if(match) return match;
  // Если ничего не найдено – вернём null (ничего не монтируем)
  return null;
}

export function listPages(){ return pages.slice(); }