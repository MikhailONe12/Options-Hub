const { createApp } = require('./server/app');
const { buildPorts, tryListen } = require('./server/utils/serverHelpers');

// Global process error handlers
process.on('uncaughtException', (err) => {
    console.error('❌ Uncaught Exception:', err.message);
    console.error(err.stack);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

const argPort = Number(process.argv[2]) || null;
const app = createApp();

tryListen(app, buildPorts(argPort)).then(server => {
    const addr = server.address();
    const port = typeof addr === 'object' ? addr.port : addr;
    console.log(`\n🚀 Modular Server on port ${port}\n   http://localhost:${port}\n`);

    console.log('Universal endpoints ready. Examples:');
    console.log('  /api/btc/tables');
    console.log('  /api/btc-metrics/latest');
    console.log('  /api/btc/gex/expirations');
    console.log('  /api/btc/volatility/strikes');
}).catch(e => {
    console.error('Start failed:', e.message);
    process.exit(1);
});
