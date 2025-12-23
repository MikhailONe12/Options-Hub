/**
 * Price Comparison Pure Renderer
 */
export function render(container, ctx) {
    const d = ctx.data;
    if (!d || !d.series) {
        container.innerHTML = `<div class="block is-empty">No Price Comparison Data Available</div>`;
        return;
    }

    const { series, metrics, meta } = d;
    const { spot, fut } = series;

    // Helper: Color logic
    const getScoreColor = (val) => {
        if (val < 30) return { border: '#f44336', text: '#8B0000', label: 'Poor' };
        if (val <= 70) return { border: '#ffc107', text: '#8a6d00', label: 'Moderate' };
        return { border: '#00d084', text: '#0b5d1e', label: 'Strong' };
    };

    const getSignalStyle = (val) => {
        if (val > 20) return { border: '#00d084', text: '#0b5d1e', label: 'Buy Signal' };
        if (val < -20) return { border: '#f44336', text: '#8B0000', label: 'Sell Signal' };
        return { border: '#ddd', text: '#8a6d00', label: 'Neutral' };
    };

    const sellStyle = getScoreColor(metrics.sellScore.value);
    const buyStyle = getScoreColor(metrics.buyScore.value);
    const signalStyle = getSignalStyle(metrics.signal.value);

    container.innerHTML = `
        <style>
            .pc-container { display:flex; gap:16px; height:100%; padding:16px; background:#fff; border-radius:8px; }
            .pc-left { width:180px; display:flex; align-items:center; justify-content:center; }
            .pc-center { flex:1; position:relative; min-width:0; }
            .pc-stats { width:140px; display:flex; flex-direction:column; gap:8px; justify-content:center; }
            .pc-prices { width:180px; display:flex; flex-direction:column; gap:16px; justify-content:center; text-align:center; }
            
            .stat-box { border:1px solid #ddd; padding:8px; text-align:center; transition: border-color 0.2s; }
            .stat-label { font-size:10px; color:#666; margin-bottom:4px; }
            .stat-value { font-size:20px; font-weight:700; color:#111; }
            .stat-note { font-size:11px; font-weight:600; margin-top:2px; }
            
            .price-val { font-size:18px; font-weight:700; margin-bottom:2px; }
            .price-delta { font-size:11px; }
            
            .pc-svg-wrap { width:100%; height:100%; overflow:hidden; }
            .pc-svg { width:100%; height:100%; cursor:crosshair; }
            
            .pc-ticker-text { font-size:18px; font-weight:700; color:#111; letter-spacing:1px; }
        </style>
        <div class="pc-container">
            <div class="pc-left">
                <div class="pc-ticker-text">${meta.ticker.replace(/([A-Z]+)(USDT)/, '$1/$2')}</div>
            </div>
            
            <div class="pc-center">
                <div class="pc-svg-wrap" id="pc-wrap">
                    <svg id="pc-svg" class="pc-svg"></svg>
                </div>
            </div>

            <div class="pc-stats">
                <div class="stat-box" style="border-color:${sellStyle.border}">
                    <div class="stat-label">Sell Score</div>
                    <div class="stat-value">${metrics.sellScore.formatted}</div>
                    <div class="stat-note" style="color:${sellStyle.text}">${sellStyle.label}</div>
                </div>
                <div class="stat-box" style="border-color:${buyStyle.border}">
                    <div class="stat-label">Buy Score</div>
                    <div class="stat-value">${metrics.buyScore.formatted}</div>
                    <div class="stat-note" style="color:${buyStyle.text}">${buyStyle.label}</div>
                </div>
                <div class="stat-box" style="border-color:${signalStyle.border}">
                    <div class="stat-label">Difference</div>
                    <div class="stat-value" style="font-size:16px">${metrics.signal.formatted}</div>
                    <div class="stat-note" style="color:${signalStyle.text}">${signalStyle.label}</div>
                </div>
            </div>

            <div class="pc-prices">
                <div>
                    <div class="stat-label">SPOT</div>
                    <div class="price-val" style="color:#0080ff">${metrics.spotPrice.toLocaleString('ru-RU')}</div>
                    <div class="price-delta" style="color:#0080ff">${metrics.spotDelta.formatted}</div>
                </div>
                <div>
                    <div class="stat-label">FUTURES</div>
                    <div class="price-val" style="color:#00d084">${metrics.futPrice.toLocaleString('ru-RU')}</div>
                    <div class="price-delta" style="color:#00d084">${metrics.futDelta.formatted}</div>
                </div>
                <div style="border-top:1px solid #eee; padding-top:8px; font-size:10px; color:#999;">
                    IV: ${(metrics.volatility.iv * 100).toFixed(2)}% • HV: ${(metrics.volatility.hv * 100).toFixed(2)}%<br>
                    Gap: ${(metrics.volatility.gap).toFixed(2)}%
                </div>
            </div>
        </div>
    `;

    drawChart(container, series);
}

function drawChart(container, series) {
    const svg = container.querySelector('#pc-svg');
    const { spot, fut } = series;

    const w = 1000;
    const h = 240;
    const extraRight = 80;
    svg.setAttribute('viewBox', `0 0 ${w + extraRight} ${h}`);

    if (!spot.length && !fut.length) {
        svg.innerHTML = `<text x="500" y="120" text-anchor="middle" fill="#999">No series data</text>`;
        return;
    }

    const all = [...spot, ...fut];
    const min = Math.min(...all);
    const max = Math.max(...all);
    const range = (max - min) || 1;
    const pad = range * 0.1;
    const sMin = min - pad;
    const sMax = max + pad;
    const sRange = sMax - sMin;

    const x0 = 70;
    const chartW = w - x0 - 10;
    const getY = p => h - ((p - sMin) / sRange) * h;
    const getX = (i, len) => len <= 1 ? x0 + chartW / 2 : x0 + (i / (len - 1)) * chartW;

    // Build Paths
    const spotPath = spot.map((p, i) => `${getX(i, spot.length)},${getY(p)}`).join(' ');
    const futPath = fut.map((p, i) => `${getX(i, fut.length)},${getY(p)}`).join(' ');

    svg.innerHTML = `
        <defs>
            <clipPath id="pc-clip"><rect id="pc-clip-rect" x="0" y="0" width="${w + extraRight}" height="${h}" /></clipPath>
        </defs>
        <g class="paths-dim">
            <polyline points="${spotPath}" fill="none" stroke="#0080ff" stroke-width="2.5" opacity="0.3" stroke-linecap="round" stroke-linejoin="round" />
            <polyline points="${futPath}" fill="none" stroke="#00d084" stroke-width="2" opacity="0.3" stroke-linecap="round" stroke-linejoin="round" />
        </g>
        <g class="paths-bright" clip-path="url(#pc-clip)">
            <polyline points="${spotPath}" fill="none" stroke="#0080ff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" />
            <polyline points="${futPath}" fill="none" stroke="#00d084" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        </g>
        <line id="pc-crosshair" x1="0" y1="0" x2="0" y2="${h}" stroke="#666" stroke-width="1" stroke-dasharray="4 2" opacity="0" />
        <text id="pc-tip-spot" opacity="0" font-size="14" font-weight="600" fill="#0080ff" text-anchor="middle" y="20"></text>
        <text id="pc-tip-fut" opacity="0" font-size="14" font-weight="600" fill="#00d084" text-anchor="middle" y="${h - 10}"></text>
    `;

    // Interaction
    const crosshair = svg.querySelector('#pc-crosshair');
    const tipSpot = svg.querySelector('#pc-tip-spot');
    const tipFut = svg.querySelector('#pc-tip-fut');
    const clipRect = svg.querySelector('#pc-clip-rect');

    svg.addEventListener('mousemove', e => {
        const pt = svg.createSVGPoint();
        pt.x = e.clientX;
        pt.y = e.clientY;
        const svgP = pt.matrixTransform(svg.getScreenCTM().inverse());
        const svgX = svgP.x;

        if (svgX < x0 || svgX > x0 + chartW) {
            crosshair.setAttribute('opacity', '0');
            tipSpot.setAttribute('opacity', '0');
            tipFut.setAttribute('opacity', '0');
            clipRect.setAttribute('width', String(w + extraRight));
            return;
        }

        crosshair.setAttribute('x1', String(svgX));
        crosshair.setAttribute('x2', String(svgX));
        crosshair.setAttribute('opacity', '0.6');
        clipRect.setAttribute('width', String(svgX));

        const ratio = (svgX - x0) / chartW;

        if (spot.length) {
            const val = spot[Math.round(ratio * (spot.length - 1))];
            tipSpot.textContent = val.toLocaleString('ru-RU');
            tipSpot.setAttribute('x', String(svgX));
            tipSpot.setAttribute('opacity', '1');
        }

        if (fut.length) {
            const val = fut[Math.round(ratio * (fut.length - 1))];
            tipFut.textContent = val.toLocaleString('ru-RU');
            tipFut.setAttribute('x', String(svgX));
            tipFut.setAttribute('opacity', '1');
        }
    });

    svg.addEventListener('mouseleave', () => {
        crosshair.setAttribute('opacity', '0');
        tipSpot.setAttribute('opacity', '0');
        tipFut.setAttribute('opacity', '0');
        clipRect.setAttribute('width', String(w + extraRight));
    });
}
