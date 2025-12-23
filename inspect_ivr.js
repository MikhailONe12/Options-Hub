const sqlite3 = require('sqlite3').verbose();
const path = 'G:/Option_Hub/0_Data/Bybit_metrics/Bybit_BTC/Bybit_BTC_Op_Metrics.db';
console.log('Opening DB:', path);
const db = new sqlite3.Database(path, sqlite3.OPEN_READONLY, (err) => {
  if (err) { console.error('Open error', err); return; }
  db.all("SELECT name FROM sqlite_master WHERE type='table'", [], (e, rows) => {
    if (e) { console.error('tables err', e); return; }
    console.log('Tables:', rows.map(r => r.name));
    const targets = ['Options_Greeks_Comulative','Options_Greeks_Cumulative','options_greeks_comulative','options_greeks_cumulative'];
    let idx = 0;
    function inspect(){
      if (idx >= targets.length) { db.close(); return; }
      const t = targets[idx++];
      db.all('PRAGMA table_info('+t+')', [], (piErr, piRows) => {
        if (piErr) { console.error('PRAGMA error for', t, piErr.message); inspect(); return; }
        console.log('Schema for', t, piRows);
        db.get('SELECT * FROM '+t+' ORDER BY id DESC LIMIT 1', [], (rowErr, row) => {
          if (rowErr) console.error('Row error for', t, rowErr.message); else console.log('Latest row for', t, row);
          inspect();
        });
      });
    }
    inspect();
  });
});
