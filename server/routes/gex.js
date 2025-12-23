const express = require('express');
const router = express.Router();
const { openDbForAsset } = require('../db/connections');
const { runAll } = require('../db/helpers');
const { cacheGet, cacheSet } = require('../cache/memory');
const { validateAsset } = require('../utils/serverHelpers');

// TEST GEX endpoint
router.get('/test-gex', (req, res) => {
    res.json({ test: 'GEX endpoint working!' });
});

// Get expirations
router.get('/:asset/gex/expirations', async (req, res) => {
    req.setTimeout(30000);
    const asset = req.params.asset.toLowerCase();
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });

    const cacheKey = `gex_expirations:${asset}`;
    const cached = cacheGet(cacheKey, 15000);
    if (cached) return res.json(cached);

    const db = openDbForAsset(asset);
    try {
        const rows = await runAll(db, `
            SELECT
                expiry_date as expiration_date,
                dte,
                contract_count,
                total_gamma,
                call_gamma,
                put_gamma
            FROM Options_Greeks_By_Expiry
            WHERE asset = ?
              AND expiry_date >= date('now', '-1 day') 
              AND (
                expiry_date > date('now') 
                OR (
                  expiry_date = date('now') 
                  AND strftime('%H', 'now') < '08'
                )
              )
            ORDER BY expiry_date
        `, [asset]);
        const out = { asset, expirations: rows || [] };
        cacheSet(cacheKey, out);
        res.json(out);
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] gex/expirations error:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

// GEX Matrix - strikes for chosen dates
router.get('/:asset/gex/strikes', async (req, res) => {
    req.setTimeout(30000);
    const asset = req.params.asset.toLowerCase();
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });

    const dates = req.query.dates;
    const mode = req.query.mode || 'single';

    if (!dates) return res.status(400).json({ error: 'dates parameter required' });

    const cacheKey = `gex_strikes:${asset}:${mode}:${dates}`;
    const cached = cacheGet(cacheKey, 15000);
    if (cached) return res.json(cached);

    const db = openDbForAsset(asset);
    try {
        const isCumulative = mode === 'all' || mode === 'cumulative' || !dates || dates.toLowerCase() === 'all';
        let query, params;

        if (isCumulative) {
            query = `SELECT strike, call_oi, put_oi, net_oi, call_gex, put_gex, net_gex, call_dex, put_dex, net_dex 
                     FROM Options_Greeks_By_Strike_All_Expiries 
                     WHERE asset = ? 
                     ORDER BY strike DESC`;
            params = [asset];
        } else {
            const dateList = dates.split(',').map(d => d.trim());
            const placeholders = dateList.map(() => '?').join(',');
            query = `SELECT strike, call_oi, put_oi, net_oi, call_gex, put_gex, net_gex, call_dex, put_dex, net_dex 
                     FROM Options_Greeks_By_Strike 
                     WHERE asset = ? AND date IN (${placeholders}) 
                     ORDER BY strike DESC`;
            params = [asset, ...dateList];
        }

        const rows = await runAll(db, query, params);

        // Node.js performs table selection only, not data logic.
        // Each endpoint maps to exactly one precomputed table per request.
        // No JS aggregation (reduce) or SQL SUM allowed here.
        const strikes = rows || [];
        const out = { asset, mode, dates: isCumulative ? 'all' : dates, strikes };
        cacheSet(cacheKey, out);
        res.json(out);
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] gex/strikes error:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

// Detailed strikes
router.get('/:asset/gex/detailed-strikes', async (req, res) => {
    req.setTimeout(30000);
    const asset = req.params.asset.toLowerCase();
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });

    const cacheKey = `gex_detailed:${asset}`;
    const cached = cacheGet(cacheKey, 30000);
    if (cached) return res.json(cached);

    const db = openDbForAsset(asset);
    try {
        const expRows = await runAll(db, `
            SELECT expiry_date
            FROM Options_Greeks_By_Expiry
            WHERE asset = ?
              AND expiry_date >= date('now', '-1 day') 
              AND (
                expiry_date > date('now') 
                OR (
                  expiry_date = date('now') 
                  AND strftime('%H', 'now') < '08'
                )
              )
            ORDER BY expiry_date
        `, [asset]);
        const expirations = expRows.map(r => r.expiry_date);

        if (!expirations.length) return res.json({ asset, expirations: [], strikes: [] });

        const strikes = await runAll(db, `
            SELECT strike, net_dex, net_gex, net_oi, call_oi, put_oi, call_gex, put_gex
            FROM Options_Greeks_By_Strike_All_Expiries
            WHERE asset = ?
            ORDER BY strike DESC
        `, [asset]);

        const out = { asset, expirations, strikes: strikes || [] };
        cacheSet(cacheKey, out);
        res.json(out);
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] gex/detailed-strikes error:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

module.exports = router;
