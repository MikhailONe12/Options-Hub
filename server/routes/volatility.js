const express = require('express');
const router = express.Router();
const { openDbForAsset } = require('../db/connections');
const { runGet, runAll } = require('../db/helpers');
const { cacheGet, cacheSet } = require('../cache/memory');
const { validateAsset } = require('../utils/serverHelpers');

// Volatility Strikes
router.get('/:asset/volatility/strikes', async (req, res) => {
    req.setTimeout(30000);
    const asset = req.params.asset.toLowerCase();
    const expirationDate = req.query.expiration;
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });

    if (expirationDate === 'undefined' || expirationDate === 'null') {
        return res.json({ asset, data: [] });
    }

    const cacheKey = `vol_strikes:${asset}:${expirationDate || 'all'}`;
    const cached = cacheGet(cacheKey, expirationDate ? 15000 : 30000);
    if (cached) return res.json(cached);

    const db = openDbForAsset(asset);
    try {
        const metrics = await runGet(db, `SELECT underlying_price FROM metrics_data ORDER BY id DESC LIMIT 1`, []);
        const spotPrice = metrics?.underlying_price || 0;

        let query, params;
        if (expirationDate) {
            query = `SELECT expiry as expiration_date, strike, type as option_type, iv as implied_volatility, 
                            bid, ask, mid as markPrice, iv as implied_volatility
                     FROM Options_Volatility 
                     WHERE asset = ? AND expiry = ?
                     ORDER BY strike ASC`;
            params = [asset, expirationDate];
        } else {
            query = `SELECT expiry as expiration_date, strike, type as option_type, iv as implied_volatility, 
                            bid, ask, mid as markPrice, iv as implied_volatility
                     FROM Options_Volatility 
                     WHERE asset = ?
                     ORDER BY expiry, strike ASC`;
            params = [asset];
        }

        const rows = await runAll(db, query, params);

        const strikeMap = {};
        rows.forEach(r => {
            if (!strikeMap[r.expiration_date]) strikeMap[r.expiration_date] = {};
            const expMap = strikeMap[r.expiration_date];
            if (!expMap[r.strike]) {
                expMap[r.strike] = {
                    strike: r.strike,
                    call: { iv: null, bid: null, ask: null, last: null, oi: null },
                    put: { iv: null, bid: null, ask: null, last: null, oi: null }
                };
            }
            const s = expMap[r.strike];
            const type = r.option_type.toLowerCase() === 'c' || r.option_type === 'call' ? 'call' : 'put';

            // Node.js MUST NOT perform calculations or aggregate data.
            // NO JS reduce, map, or arithmetic.
            // Directly use r.bid, r.ask, r.mid from the database result.
            s[type] = {
                iv: r.implied_volatility !== null ? Number(r.implied_volatility) : null,
                bid: r.bid !== null ? Number(r.bid) : null,
                ask: r.ask !== null ? Number(r.ask) : null,
                last: r.markPrice !== null ? Number(r.markPrice) : null,
                oi: null // OI not required in Options_Volatility for now, but kept for schema compatibility
            };
        });

        const expirations = Object.keys(strikeMap).sort();
        const data = expirations.map(exp => {
            const strikes = Object.values(strikeMap[exp]).sort((a, b) => a.strike - b.strike);
            return { expiration: exp, spotPrice: Number(spotPrice), strikes };
        });
        const out = { asset, data };
        cacheSet(cacheKey, out);
        res.json(out);
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] volatility/strikes error:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

module.exports = router;
