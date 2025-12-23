const express = require('express');
const router = express.Router();
const { openDbForAsset } = require('../db/connections');
const { runGet, runAll } = require('../db/helpers');
const { cacheGet, cacheSet } = require('../cache/memory');

const assets = ['btc', 'eth', 'sol', 'xrp', 'mnt', 'doge', 'imoex'];

assets.forEach(asset => {
    // Latest metrics
    router.get(`/${asset}-metrics/latest`, (req, res) => {
        req.setTimeout(30000);
        const cacheKey = `metrics_latest:${asset}`;
        const cached = cacheGet(cacheKey, 5000);
        if (cached) return res.json(cached);

        const db = openDbForAsset(asset);
        (async () => {
            try {
                const metrics = await runGet(db, `SELECT * FROM metrics_data ORDER BY id DESC LIMIT 1`, []);
                const cumu = await runGet(db, `SELECT IVR, sum_OI_calls, sum_OI_puts, max_pain, Sell_Score, Buy_Score, IV_pct, RV_pct, HV_pct, Gap, w_iv_sell, w_rv_sell, w_hv_sell, w_iv_buy, w_rv_buy, w_hv_buy, realized_volatility, historical_volatility, sum_Delta_OI, sum_Gamma_OI, sum_Vega_OI, sum_Theta_OI, sum_Vanna_OI, sum_Charm_OI, sum_Volga_OI, sum_Veta_OI, sum_Speed_OI, sum_Zomma_OI, sum_Color_OI, sum_Ultima_OI FROM Options_Greeks_Comulative ORDER BY id DESC LIMIT 1`, []);

                const response = Object.assign({}, metrics || {}, {
                    ivr: cumu?.IVR ? Number(cumu.IVR) : 0,
                    callOi: cumu?.sum_OI_calls ? Number(cumu.sum_OI_calls) : 0,
                    putOi: cumu?.sum_OI_puts ? Number(cumu.sum_OI_puts) : 0,
                    // Greeks
                    sum_Delta_OI: cumu?.sum_Delta_OI ? Number(cumu.sum_Delta_OI) : 0,
                    sum_Gamma_OI: cumu?.sum_Gamma_OI ? Number(cumu.sum_Gamma_OI) : 0,
                    sum_Vega_OI: cumu?.sum_Vega_OI ? Number(cumu.sum_Vega_OI) : 0,
                    sum_Theta_OI: cumu?.sum_Theta_OI ? Number(cumu.sum_Theta_OI) : 0,
                    sum_Vanna_OI: cumu?.sum_Vanna_OI ? Number(cumu.sum_Vanna_OI) : 0,
                    sum_Charm_OI: cumu?.sum_Charm_OI ? Number(cumu.sum_Charm_OI) : 0,
                    sum_Volga_OI: cumu?.sum_Volga_OI ? Number(cumu.sum_Volga_OI) : 0,
                    sum_Veta_OI: cumu?.sum_Veta_OI ? Number(cumu.sum_Veta_OI) : 0,
                    sum_Speed_OI: cumu?.sum_Speed_OI ? Number(cumu.sum_Speed_OI) : 0,
                    sum_Zomma_OI: cumu?.sum_Zomma_OI ? Number(cumu.sum_Zomma_OI) : 0,
                    sum_Color_OI: cumu?.sum_Color_OI ? Number(cumu.sum_Color_OI) : 0,
                    sum_Ultima_OI: cumu?.sum_Ultima_OI ? Number(cumu.sum_Ultima_OI) : 0,
                    max_pain: cumu?.max_pain ? Number(cumu.max_pain) : 0,

                    // Enhanced IVR metrics
                    Sell_Score: cumu?.Sell_Score ? Number(cumu.Sell_Score) : null,
                    Buy_Score: cumu?.Buy_Score ? Number(cumu.Buy_Score) : null,
                    IV_pct: cumu?.IV_pct ? Number(cumu.IV_pct) : null,
                    RV_pct: cumu?.RV_pct ? Number(cumu.RV_pct) : null,
                    HV_pct: cumu?.HV_pct ? Number(cumu.HV_pct) : null,
                    Gap: cumu?.Gap ? Number(cumu.Gap) : null,
                    w_iv_sell: cumu?.w_iv_sell ? Number(cumu.w_iv_sell) : null,
                    w_rv_sell: cumu?.w_rv_sell ? Number(cumu.w_rv_sell) : null,
                    w_hv_sell: cumu?.w_hv_sell ? Number(cumu.w_hv_sell) : null,
                    w_iv_buy: cumu?.w_iv_buy ? Number(cumu.w_iv_buy) : null,
                    w_rv_buy: cumu?.w_rv_buy ? Number(cumu.w_rv_buy) : null,
                    w_hv_buy: cumu?.w_hv_buy ? Number(cumu.w_hv_buy) : null,
                    realized_volatility: cumu?.realized_volatility ? Number(cumu.realized_volatility) : null,
                    historical_volatility: cumu?.historical_volatility ? Number(cumu.historical_volatility) : null
                });
                cacheSet(cacheKey, response);
                res.json(response);
            } catch (e) {
                console.error(`[${asset.toUpperCase()}] metrics/latest error:`, e.message);
                res.status(500).json({ error: e.message });
            }
        })();
    });

    // History metrics
    router.get(`/${asset}-metrics/latest/:count`, (req, res) => {
        req.setTimeout(30000);
        const count = parseInt(req.params.count) || 10;
        const cacheKey = `metrics_history:${asset}:${count}`;
        const cached = cacheGet(cacheKey, 5000);
        if (cached) return res.json(cached);

        const db = openDbForAsset(asset);
        runAll(db, `SELECT * FROM metrics_data ORDER BY id DESC LIMIT ?`, [count])
            .then(rows => {
                const out = (rows || []).reverse();
                cacheSet(cacheKey, out);
                res.json(out);
            })
            .catch(e => {
                console.error(`[${asset.toUpperCase()}] metrics_data history error:`, e.message);
                res.status(500).json({ error: e.message });
            });
    });
});

module.exports = router;
