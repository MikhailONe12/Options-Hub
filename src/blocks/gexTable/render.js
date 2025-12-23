/**
 * GEX Table Pure Renderer
 */
export function render(container, ctx) {
    const { expirations, currentMode, selectedExpiry } = ctx.data || { expirations: [], currentMode: 'single', selectedExpiry: null };
    const { theme } = ctx.ui || { theme: 'dark' };

    if (!expirations || expirations.length === 0) {
        container.innerHTML = '<div class="table-empty">Loading expirations...</div>';
        return;
    }

    const maxGamma = Math.max(...expirations.map(e => Math.abs(e.totalGamma)), 1);

    const modeButtons = `
        <div class="gex-mode-toggles" style="display:flex; flex-direction:column; gap:8px;">
            <button class="mode-btn ${currentMode === 'cumulative' ? 'is-active' : ''}" data-mode="cumulative">
                ∑ SUM
            </button>
            <button class="mode-btn ${currentMode === 'single' ? 'is-active' : ''}" data-mode="single">
                ● SINGLE
            </button>
        </div>
    `;

    const datesList = expirations.map(exp => {
        const isSelected = exp.expiry === selectedExpiry;
        const gammaLevel = Math.abs(exp.totalGamma) / maxGamma;
        const gammaColor = exp.totalGamma >= 0 ? '#4caf50' : '#f44336';

        // Date formatting: DD/MM/YY
        const dateObj = new Date(exp.expiry);
        const dateStr = `${dateObj.getDate()}/${dateObj.getMonth() + 1}/${dateObj.getFullYear().toString().slice(-2)}`;

        return `
            <div class="expiry-item ${isSelected ? 'is-selected' : ''}" data-expiry="${exp.expiry}">
                <div class="expiry-date">${dateStr}</div>
                <div class="expiry-dte">${exp.dte} DTE ${exp.isWeekly ? '• W' : ''}</div>
                <div class="gamma-dot" style="background:${gammaColor}; opacity:${0.3 + (gammaLevel * 0.7)};"></div>
                <div class="gamma-bar-wrap">
                    <div class="gamma-bar" style="height:${gammaLevel * 25}px; background:${gammaColor};"></div>
                </div>
                <div class="gamma-value" style="color:${gammaColor}">${(exp.totalGamma / 1000).toFixed(1)}K</div>
            </div>
        `;
    }).join('');

    container.innerHTML = `
        <style>
            .gex-table-block { background:#fff; padding:12px; display:flex; flex-direction:column; gap:12px; }
            .gex-table-header { font-size:12px; font-weight:700; color:#111; }
            .gex-table-content { display:flex; gap:16px; align-items:flex-start; }
            .mode-btn { 
                padding:10px 14px; background:#f5f5f5; color:#666; border:none; border-radius:4px; 
                cursor:pointer; font-size:11px; font-weight:600; transition:0.2s; white-space:nowrap;
            }
            .mode-btn.is-active { background:#2196f3; color:#fff; }
            .gex-dates { display:flex; gap:10px; flex:1; overflow-x:auto; padding-bottom:8px; }
            
            .expiry-item {
                display:flex; flex-direction:column; align-items:center; justify-content:space-between;
                padding:8px; background:#fafafa; border-radius:6px; cursor:pointer;
                min-width:75px; height:110px; border:2px solid transparent; transition:0.2s;
            }
            .expiry-item:hover { background: #f0f7ff; }
            .expiry-item.is-selected { background:#e3f2fd; border-color:#2196f3; }
            
            .expiry-date { font-size:12px; font-weight:700; }
            .expiry-dte { font-size:9px; opacity:0.7; }
            .gamma-dot { width:8px; height:8px; border-radius:50%; margin:4px 0; }
            .gamma-bar-wrap { flex:1; display:flex; align-items:flex-end; }
            .gamma-bar { width:3px; border-radius:2px; }
            .gamma-value { font-size:9px; font-weight:600; margin-top:4px; }
        </style>
        <div class="gex-table-block">
            <div class="gex-table-header">GEX LIVE - EXPIRES</div>
            <div class="gex-table-content">
                ${modeButtons}
                <div class="gex-dates">
                    ${datesList}
                </div>
            </div>
        </div>
    `;
}
