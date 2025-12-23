function listTables(db) {
    return new Promise((resolve, reject) => {
        db.all(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, [], (e, rows) => {
            if (e) return reject(e); resolve(rows.map(r => r.name));
        });
    });
}

function listColumns(db, table) {
    return new Promise((resolve, reject) => {
        db.all(`PRAGMA table_info(${table})`, [], (e, rows) => {
            if (e) return reject(e); resolve(rows.map(r => r.name));
        });
    });
}

function runQuery(db, sql, params) {
    const startTime = Date.now();
    return new Promise((resolve, reject) => {
        db.all(sql, params, (e, rows) => {
            const duration = Date.now() - startTime;
            if (duration > 5000) {
                console.warn(`[SLOW QUERY] Query took ${duration}ms: ${sql.substring(0, 100)}...`);
            }
            if (e) reject(e); else resolve(rows);
        });
    });
}

function runGet(db, sql, params) {
    const startTime = Date.now();
    return new Promise((resolve, reject) => {
        db.get(sql, params, (e, row) => {
            const duration = Date.now() - startTime;
            if (duration > 5000) {
                console.warn(`[SLOW QUERY] Query took ${duration}ms: ${sql.substring(0, 100)}...`);
            }
            if (e) reject(e); else resolve(row);
        });
    });
}

function runAll(db, sql, params) {
    const startTime = Date.now();
    return new Promise((resolve, reject) => {
        db.all(sql, params, (e, rows) => {
            const duration = Date.now() - startTime;
            if (duration > 5000) {
                console.warn(`[SLOW QUERY] Query took ${duration}ms: ${sql.substring(0, 100)}...`);
            }
            if (e) reject(e); else resolve(rows);
        });
    });
}

module.exports = {
    listTables,
    listColumns,
    runQuery,
    runGet,
    runAll
};
