/**
 * Pure DOM renderer for GEX Matrix Block.
 * Implements Thin Block Contract v1.0 (Extended).
 */
export function render(container, ctx) {
    if (!ctx.data) {
        container.innerHTML = '<div class="matrix-empty">Loading Matrix Data...</div>';
        return;
    }

    const { strikes, cells, maxVals, spotPrice, ticker } = ctx.data;
    const { theme } = ctx.ui;

    const fmt = (v, decimals = 0) => {
        if (v == null || isNaN(v)) return '--';
        const absV = Math.abs(v);
        if (absV >= 1e9) return (v / 1e9).toFixed(1) + 'B';
        if (absV >= 1e6) return (v / 1e6).toFixed(1) + 'M';
        if (absV >= 1e3) return (v / 1e3).toFixed(1) + 'K';
        return Number(v).toFixed(decimals);
    };

    const headerStyle = "font-size:10px;font-weight:700;text-transform:uppercase;opacity:0.7;padding:6px 4px;text-align:center;border-bottom:2px solid #e0e0e0;";

    const { resistance, support } = ctx.data.keyLevels || {};

    const rows = strikes.map((strike, index) => {
        // Find the data for this strike (using first expiry as proxy since this is a matrix)
        // In a real matrix, we might need a more complex lookup
        const expiry = ctx.data.expiries[0];
        const cell = cells[`${strike}:${expiry}`] || {
            dex: { width: 0, sign: 1 },
            gex: { width: 0, sign: 1 },
            netOi: { width: 0, sign: 1 },
            callOi: { width: 0 },
            putOi: { width: 0 }
        };

        const dexColor = cell.dex.sign >= 0 ? '#4caf50' : '#f44336';
        const gexColor = cell.gex.sign >= 0 ? '#4caf50' : '#f44336';
        const netOiColor = cell.netOi.sign >= 0 ? '#4caf50' : '#f44336';

        // Check if this strike is a key level
        const isResistance = strike === resistance;
        const isSupport = strike === support;

        return `
            <div class="matrix-row ${isResistance ? 'is-resistance' : ''} ${isSupport ? 'is-support' : ''}"
                 data-strike="${strike}"
                 style="display:contents;">
                <div class="matrix-cell matrix-cell--strike" style="padding:3px 6px;text-align:right;font-weight:500;font-size:11px; position:relative;">
                    ${strike.toLocaleString()}
                </div>
                
                <!-- Net DEX -->
                <div class="matrix-cell" style="padding:3px 2px;position:relative;">
                    <div style="height:12px;width:${cell.dex.width}%;background:${dexColor};border-radius:2px;opacity:0.8;"></div>
                </div>
                
                <!-- Net GEX (Centered) -->
                <div class="matrix-cell" style="padding:3px 2px;position:relative;display:flex;align-items:center;justify-content:center;">
                    <div style="width:100%;height:12px;position:relative;">
                        <div style="position:absolute;left:50%;top:0;bottom:0;width:1px;background:#999;transform:translateX(-50%);z-index:1;"></div>
                        ${cell.gex.sign < 0
                ? `<div style="position:absolute;right:50%;top:0;bottom:0;width:${cell.gex.width}%;background:${gexColor};border-radius:2px;opacity:0.8;"></div>`
                : `<div style="position:absolute;left:50%;top:0;bottom:0;width:${cell.gex.width}%;background:${gexColor};border-radius:2px;opacity:0.8;"></div>`
            }
                    </div>
                </div>
                
                <!-- Net OI -->
                <div class="matrix-cell" style="padding:3px 2px;position:relative;">
                    <div style="height:12px;width:${cell.netOi.width}%;background:${netOiColor};border-radius:2px;opacity:0.8;"></div>
                </div>
                
                <!-- OI Split -->
                <div class="matrix-cell" style="padding:3px 2px;position:relative;display:flex;justify-content:center;align-items:center;">
                    <div style="width:50%;display:flex;justify-content:flex-end;padding-right:1px;">
                        <div style="height:12px;width:${cell.callOi.width}%;background:#4caf50;border-radius:2px;opacity:0.7;"></div>
                    </div>
                    <div style="width:50%;display:flex;justify-content:flex-start;padding-left:1px;">
                        <div style="height:12px;width:${cell.putOi.width}%;background:#f44336;border-radius:2px;opacity:0.7;"></div>
                    </div>
                </div>
            </div>
        `;
    }).join('');

    container.innerHTML = `
        <style>
            .matrix-row.is-resistance .matrix-cell--strike::after {
                content: 'CR'; position: absolute; left: 0; top: 0; font-size: 8px; color: #4caf50; font-weight: bold;
            }
            .matrix-row.is-support .matrix-cell--strike::after {
                content: 'PS'; position: absolute; left: 0; top: 0; font-size: 8px; color: #f44336; font-weight: bold;
            }
            .spot-line {
                position: absolute; left: 0; right: 0; height: 2px; background: #2196f3; 
                z-index: 100; pointer-events: none; opacity: 0.8;
                box-shadow: 0 0 8px rgba(33, 150, 243, 0.5);
            }
            .spot-label {
                position: absolute; right: 0; background: #2196f3; color: white; 
                padding: 2px 6px; font-size: 10px; font-weight:bold; border-radius: 4px 0 0 4px;
                transform: translateY(-50%);
            }
        </style>
        <div class="gex-matrix-block gex-matrix--${theme}" style="height:100%; display:flex; flex-direction:column; overflow:hidden; background:#fff;">
            <div class="block-title">GEX MATRIX - ${ticker}</div>
            
            <div class="matrix-header" style="flex-shrink:0;">
                <div style="display:grid; grid-template-columns: 120px repeat(3, 1fr) 1fr; gap:0; padding:0 10px;">
                    <div style="${headerStyle}">Strike</div>
                    <div style="${headerStyle}">Net DEX</div>
                    <div style="${headerStyle}">Net GEX</div>
                    <div style="${headerStyle}">Net OI</div>
                    <div style="${headerStyle}">OI (C/P)</div>
                </div>
            </div>
            
            <div class="matrix-body" style="flex:1; overflow-y:auto; position:relative; padding:0 10px;">
                <div style="display:grid; grid-template-columns: 120px repeat(3, 1fr) 1fr; gap:0; position:relative;">
                    ${rows}
                    <div id="matrix-spot-line" class="spot-line" style="display:none;">
                        <div class="spot-label">${fmt(spotPrice, 2)}</div>
                    </div>
                </div>
            </div>
        </div>
    `;

    // Position spot line
    const rowsArr = Array.from(container.querySelectorAll('.matrix-row'));
    let closestRow = null;
    let minDiff = Infinity;
    rowsArr.forEach(row => {
        const strike = parseFloat(row.dataset.strike);
        const diff = Math.abs(strike - spotPrice);
        if (diff < minDiff) {
            minDiff = diff;
            closestRow = row;
        }
    });

    if (closestRow) {
        const line = container.querySelector('#matrix-spot-line');
        if (line) {
            line.style.display = 'block';
            line.style.top = `${closestRow.offsetTop + (closestRow.offsetHeight / 2)}px`;
        }
    }
}
