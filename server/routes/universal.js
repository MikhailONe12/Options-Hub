const express = require('express');
const router = express.Router();
const { dbMap } = require('../db/connections');
const { runGet, runQuery, listTables, listColumns } = require('../db/helpers');
const { validateAsset, validateTableName } = require('../utils/serverHelpers');

// List tables
router.get('/:asset/tables', async (req, res) => {
    const asset = req.params.asset.toLowerCase();
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });
    try {
        res.json({ asset, tables: await listTables(dbMap[asset]) });
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] list tables error:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

// Table columns
router.get('/:asset/:table/columns', async (req, res) => {
    const asset = req.params.asset.toLowerCase();
    const table = req.params.table;
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });
    if (!validateTableName(table)) return res.status(400).json({ error: 'Bad table name' });
    try {
        res.json({ asset, table, columns: await listColumns(dbMap[asset], table) });
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] list columns error for ${table}:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

// Latest row
router.get('/:asset/:table/latest', async (req, res) => {
    req.setTimeout(30000);
    const asset = req.params.asset.toLowerCase();
    const table = req.params.table;
    const fields = req.query.fields ? req.query.fields.split(',').filter(validateTableName) : null;
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });
    if (!validateTableName(table)) return res.status(400).json({ error: 'Bad table name' });
    try {
        const cols = fields ? fields.join(',') : '*';
        const row = await runGet(dbMap[asset], `SELECT ${cols} FROM ${table} ORDER BY id DESC LIMIT 1`, []);
        res.json(row || {});
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] latest row error for ${table}:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

// Latest N rows
router.get('/:asset/:table/latest/:count', async (req, res) => {
    req.setTimeout(30000);
    const asset = req.params.asset.toLowerCase();
    const table = req.params.table;
    const count = parseInt(req.params.count) || 10;
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });
    if (!validateTableName(table)) return res.status(400).json({ error: 'Bad table name' });
    try {
        const rows = await runQuery(dbMap[asset], `SELECT * FROM ${table} ORDER BY id DESC LIMIT ?`, [count]);
        res.json(rows.reverse());
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] latest N rows error for ${table}:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

// Row by id
router.get('/:asset/:table/id/:id', async (req, res) => {
    const asset = req.params.asset.toLowerCase();
    const table = req.params.table;
    const id = parseInt(req.params.id);
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });
    if (!validateTableName(table)) return res.status(400).json({ error: 'Bad table name' });
    if (isNaN(id)) return res.status(400).json({ error: 'Bad id' });
    try {
        const row = await runGet(dbMap[asset], `SELECT * FROM ${table} WHERE id = ?`, [id]);
        res.json(row || {});
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] row by id error for ${table}:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

// Search simple
router.get('/:asset/:table/search', async (req, res) => {
    const asset = req.params.asset.toLowerCase();
    const table = req.params.table;
    const col = req.query.col;
    const value = req.query.value;
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });
    if (!validateTableName(table)) return res.status(400).json({ error: 'Bad table name' });
    if (!col || !validateTableName(col)) return res.status(400).json({ error: 'Bad column' });
    try {
        const rows = await runQuery(dbMap[asset], `SELECT * FROM ${table} WHERE ${col} LIKE ? LIMIT 50`, [`%${value || ''}%`]);
        res.json(rows);
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] search error for ${table}.${col}:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

// Combined metrics_cumulative
router.get('/:asset/combined/metrics_cumulative', async (req, res) => {
    const asset = req.params.asset.toLowerCase();
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });
    const db = dbMap[asset];
    try {
        const metrics = await runGet(db, `SELECT * FROM metrics_data ORDER BY id DESC LIMIT 1`, []);
        const cumu = await runGet(db, `SELECT IVR, sum_OI_calls, sum_OI_puts, put_call_OI_ratio, max_pain FROM Options_Greeks_Comulative ORDER BY id DESC LIMIT 1`, []);
        res.json({ asset, metrics: metrics || {}, cumulative: cumu || {} });
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] combined metrics_cumulative error:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

// Combined greeks_maxpain
router.get('/:asset/combined/greeks_maxpain', async (req, res) => {
    const asset = req.params.asset.toLowerCase();
    if (!validateAsset(asset)) return res.status(404).json({ error: 'Unknown asset' });
    const db = dbMap[asset];
    try {
        const data = await runGet(db, `SELECT sum_Gamma_OI, sum_Vega_OI, sum_Theta_OI, sum_Vanna_OI, sum_Charm_OI, sum_Volga_OI, sum_Veta_OI, sum_Speed_OI, sum_Zomma_OI, sum_Color_OI, sum_Ultima_OI, max_pain FROM Options_Greeks_Comulative ORDER BY id DESC LIMIT 1`, []);
        res.json({ asset, greeks: data || {}, maxPain: data?.max_pain || 0 });
    } catch (e) {
        console.error(`[${asset.toUpperCase()}] greeks_maxpain error:`, e.message);
        res.status(500).json({ error: e.message });
    }
});

module.exports = router;
