const { dbMap } = require('../db/connections');

function validateAsset(asset) {
    return Object.keys(dbMap).includes(asset);
}

function validateTableName(name) {
    return /^[A-Za-z0-9_]+$/.test(name);
}

function unique(arr) {
    const s = new Set();
    return arr.filter(x => !s.has(x) && s.add(x));
}

function buildPorts(argPort) {
    const base = argPort ? [argPort] : [];
    base.push(9000, 9004, 9001);
    for (let p = 8995; p <= 9010; p++) base.push(p);
    base.push(0);
    return unique(base);
}

function tryListen(app, ports) {
    return new Promise((resolve, reject) => {
        let i = 0, server;
        const attempt = () => {
            if (i >= ports.length) return reject(new Error('No free ports'));
            const port = ports[i++];
            server = app.listen(port, () => resolve(server));
            server.on('error', e => {
                if (e.code === 'EADDRINUSE' || e.code === 'EACCES') {
                    try { server.close(); } catch (_) { }
                    setTimeout(attempt, 50);
                } else reject(e);
            });
        };
        attempt();
    });
}

module.exports = {
    validateAsset,
    validateTableName,
    unique,
    buildPorts,
    tryListen
};
