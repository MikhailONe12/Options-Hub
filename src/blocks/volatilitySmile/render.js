/**
 * Volatility Smile Pure Renderer
 */
export function render(container, ctx) {
    const { smile, meta } = ctx.data || { smile: null, meta: { hasData: false } };
    const { title = 'VOLATILITY SMILE' } = ctx.ui.props || {};

    if (!meta.hasData || !smile) {
        container.innerHTML = `
            <div class="smile-block is-empty">
                <div class="smile-header">${title}</div>
                <div class="smile-empty-msg">
                    <div class="icon">📊</div>
                    <span>No Volatility Data Available</span>
                    <small>${meta.expiration || 'N/A'}</small>
                </div>
            </div>
        `;
        return;
    }

    const { calls, puts, atm } = smile;

    // SVG Setup
    const w = 600; const h = 300;
    const margin = { top: 20, right: 30, bottom: 40, left: 60 };
    const chartW = w - margin.left - margin.right;
    const chartH = h - margin.top - margin.bottom;

    // Scaling Logic
    const allPoints = [...calls, ...puts];
    if (atm) allPoints.push(atm);

    const moneynessValues = allPoints.map(p => p.moneyness);
    const ivValues = allPoints.map(p => p.iv);

    const minM = Math.min(...moneynessValues);
    const maxM = Math.max(...moneynessValues);
    const minIv = Math.min(...ivValues);
    const maxIv = Math.max(...ivValues);

    const mPadding = (maxM - minM) * 0.1 || 0.1;
    const ivPadding = (maxIv - minIv) * 0.1 || 0.1;

    const mRange = (maxM - minM) + (2 * mPadding);
    const ivRange = (maxIv - minIv) + (2 * ivPadding);

    const getX = (m) => ((m - minM + mPadding) / mRange) * chartW + margin.left;
    const getY = (iv) => chartH - (((iv - minIv + ivPadding) / ivRange) * chartH) + margin.top;

    // SVG Generation
    function createPath(points, color, width = 2, opacity = 0.7) {
        if (points.length < 2) return '';
        const d = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${getX(p.moneyness)} ${getY(p.iv)}`).join(' ');
        return `<path d="${d}" fill="none" stroke="${color}" stroke-width="${width}" opacity="${opacity}" />`;
    }

    const callsPath = createPath(calls, '#4caf50');
    const putsPath = createPath(puts, '#f44336');
    const atmLine = atm ? `<line x1="${getX(atm.moneyness)}" y1="${margin.top}" x2="${getX(atm.moneyness)}" y2="${h - margin.bottom}" stroke="#ff9800" stroke-width="2" stroke-dasharray="5,5" />` : '';

    // Axis and Grid
    const gridRows = 5;
    const gridLinesY = Array.from({ length: gridRows + 1 }).map((_, i) => {
        const y = margin.top + (i * chartH / gridRows);
        const val = (maxIv + ivPadding) - (i * ivRange / gridRows);
        return `
            <line x1="${margin.left}" y1="${y}" x2="${w - margin.right}" y2="${y}" stroke="#f0f0f0" />
            <text x="${margin.left - 10}" y="${y + 3}" text-anchor="end" font-size="10" fill="#999">${(val * 100).toFixed(1)}%</text>
        `;
    }).join('');

    const gridCols = 4;
    const gridLinesX = Array.from({ length: gridCols + 1 }).map((_, i) => {
        const x = margin.left + (i * chartW / gridCols);
        const val = (minM - mPadding) + (i * mRange / gridCols);
        return `
            <line x1="${x}" y1="${margin.top}" x2="${x}" y2="${h - margin.bottom}" stroke="#f0f0f0" />
            <text x="${x}" y="${h - 10}" text-anchor="middle" font-size="10" fill="#999">${val.toFixed(2)}</text>
        `;
    }).join('');

    container.innerHTML = `
        <style>
            .smile-block { background:#fff; padding:16px; height:100%; display:flex; flex-direction:column; border-radius:8px; }
            .smile-header { font-size:12px; font-weight:700; color:#111; margin-bottom:16px; border-bottom:1px solid #f0f0f0; padding-bottom:8px; }
            .smile-empty-msg { flex:1; display:flex; flex-direction:column; align-items:center; justify-content:center; opacity:0.6; }
            .smile-chart-wrap { flex:1; position:relative; min-height:0; overflow:hidden; }
            .smile-svg { width:100%; height:100%; cursor:crosshair; }
            .smile-legend { display:flex; justify-content:center; gap:16px; font-size:11px; margin-top:12px; color:#666; }
            .legend-item { display:flex; align-items:center; gap:4px; }
            .legend-line { width:12px; height:2px; border-radius:1px; }
            
            .smile-tooltip {
                position:absolute; background:rgba(0,0,0,0.85); color:#fff; padding:8px 12px;
                border-radius:6px; font-size:11px; pointer-events:none; z-index:100;
                display:none; box-shadow:0 4px 12px rgba(0,0,0,0.2);
            }
        </style>
        <div class="smile-block">
            <div class="smile-header">
                ${title} <span style="font-weight:400; font-size:10px; opacity:0.6; margin-left:8px;">${meta.expiration}</span>
            </div>
            <div class="smile-chart-wrap">
                <div class="smile-tooltip" id="smile-tooltip"></div>
                <svg viewBox="0 0 ${w} ${h}" class="smile-svg" id="smile-svg">
                    <g class="grid">${gridLinesY}${gridLinesX}</g>
                    <g class="axes">
                        <text x="${w / 2}" y="${h - 5}" text-anchor="middle" font-size="11" fill="#666">Moneyness (Strike/Spot)</text>
                    </g>
                    <g class="paths">${callsPath}${putsPath}${atmLine}</g>
                </svg>
            </div>
            <div class="smile-legend">
                <div class="legend-item"><div class="legend-line" style="background:#4caf50;"></div> Calls</div>
                <div class="legend-item"><div class="legend-line" style="background:#f44336;"></div> Puts</div>
                <div class="legend-item"><div class="legend-line" style="background:#ff9800; border-top:2px dashed #ff9800;"></div> ATM</div>
            </div>
        </div>
    `;

    // Tooltip Logic
    const svg = container.querySelector('#smile-svg');
    const tooltip = container.querySelector('#smile-tooltip');

    svg.addEventListener('mousemove', (e) => {
        const rect = svg.getBoundingClientRect();
        const mouseX = ((e.clientX - rect.left) / rect.width) * w;

        // Find nearest point
        let nearest = null;
        let minDist = Infinity;

        allPoints.forEach(p => {
            const px = getX(p.moneyness);
            const dist = Math.abs(px - mouseX);
            if (dist < minDist) {
                minDist = dist;
                nearest = p;
            }
        });

        if (nearest && minDist < 20) {
            tooltip.style.display = 'block';
            tooltip.style.left = `${e.clientX - rect.left + 15}px`;
            tooltip.style.top = `${e.clientY - rect.top - 20}px`;
            tooltip.innerHTML = `
                <div style="font-weight:700; margin-bottom:4px;">Strike: ${nearest.strike}</div>
                <div>IV: ${(nearest.iv * 100).toFixed(2)}%</div>
                <div style="opacity:0.7;">Moneyness: ${nearest.moneyness.toFixed(3)}</div>
            `;
        } else {
            tooltip.style.display = 'none';
        }
    });

    svg.addEventListener('mouseleave', () => {
        tooltip.style.display = 'none';
    });
}
