const express = require('express');
const cors = require('cors');
const { initDbConnections } = require('./db/connections');
const metricsRoutes = require('./routes/metrics');
const gexRoutes = require('./routes/gex');
const volatilityRoutes = require('./routes/volatility');
const universalRoutes = require('./routes/universal');

function createApp() {
    const app = express();

    // Middleware
    app.use(cors());
    app.use(express.static('.'));
    app.use(express.json());

    // Initialize Database Connections
    initDbConnections();

    // Routes
    app.use('/api', metricsRoutes);
    app.use('/api', gexRoutes);
    app.use('/api', volatilityRoutes);
    app.use('/api', universalRoutes);

    // Global Error Handler
    app.use((err, req, res, next) => {
        console.error('❌ Express Error Handler:', err.message);
        console.error(err.stack);
        res.status(500).json({ error: 'Internal server error', message: err.message });
    });

    return app;
}

module.exports = { createApp };
