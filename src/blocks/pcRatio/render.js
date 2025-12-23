/**
 * PC Ratio Pure Renderer
 */
export function render(container, ctx) {
    const data = ctx.data;
    if (!data) {
        container.innerHTML = '<div class="block-empty">No Data</div>';
        return;
    }

    const {
        formattedRatio, formattedCalls, formattedPuts, formattedNet,
        callPct, putPct, color
    } = data;
    const { title = 'P/C RATIO' } = ctx.ui.props || {};

    const w = 240; const h = 240; const cx = w / 2; const cy = h / 2;
    const outerR = 100; const innerR = 60;

    // SVG Donut Logic
    const startAngle = -Math.PI / 2;
    const callAngle = (callPct / 100) * Math.PI * 2;
    const callEndAngle = startAngle + callAngle;
    const putEndAngle = startAngle + Math.PI * 2;

    function getPath(sA, eA) {
        const sxO = cx + outerR * Math.cos(sA);
        const syO = cy + outerR * Math.sin(sA);
        const exO = cx + outerR * Math.cos(eA);
        const eyO = cy + outerR * Math.sin(eA);
        const largeArc = (eA - sA) > Math.PI ? 1 : 0;
        const sxI = cx + innerR * Math.cos(eA);
        const syI = cy + innerR * Math.sin(eA);
        const exI = cx + innerR * Math.cos(sA);
        const eyI = cy + innerR * Math.sin(sA);
        return `M ${sxO} ${syO} A ${outerR} ${outerR} 0 ${largeArc} 1 ${exO} ${eyO} L ${sxI} ${syI} A ${innerR} ${innerR} 0 ${largeArc} 0 ${exI} ${eyI} Z`;
    }

    const callPath = getPath(startAngle, callEndAngle);
    const putPath = getPath(callEndAngle, putEndAngle);

    const netColor = color === 'positive' ? '#4caf50' : '#f44336';

    container.innerHTML = `
        <style>
            .pc-ratio-block { background:#fff; padding:12px; height:100%; display:flex; flex-direction:column; align-items:center; }
            .pc-ratio-header { width:100%; font-size:12px; font-weight:700; margin-bottom:12px; color:#111; }
            .pc-ratio-svg-wrap { flex:1; display:flex; align-items:center; justify-content:center; width:100%; min-height:0; }
            .pc-ratio-details { width:100%; font-size:10px; margin-top:-20px; }
            .pc-row { display:flex; justify-content:space-between; padding:2px 0; border-bottom:1px solid #f0f0f0; }
            .pc-label { opacity:0.6; }
            .pc-val { font-weight:600; }
        </style>
        <div class="pc-ratio-block">
            <div class="pc-ratio-header">${title}</div>
            <div class="pc-ratio-svg-wrap">
                <svg viewBox="0 0 ${w} ${h}" style="width:100%; height:100%; max-width:200px;">
                    <path d="${callPath}" fill="#4caf50" opacity="0.8"></path>
                    <path d="${putPath}" fill="#f44336" opacity="0.8"></path>
                    <text x="${cx}" y="${cy - 5}" text-anchor="middle" dominant-baseline="middle" style="font-size:24px; font-weight:700; fill:#111;">${formattedRatio}</text>
                    <text x="${cx}" y="${cy + 15}" text-anchor="middle" style="font-size:10px; fill:#666;">P/C RATIO</text>
                </svg>
            </div>
            <div class="pc-ratio-details">
                <div class="pc-row">
                    <span class="pc-label">CALLS OI:</span>
                    <span class="pc-val">${formattedCalls}</span>
                </div>
                <div class="pc-row">
                    <span class="pc-label">PUTS OI:</span>
                    <span class="pc-val">${formattedPuts}</span>
                </div>
                <div class="pc-row">
                    <span class="pc-label">NET (C-P):</span>
                    <span class="pc-val" style="color:${netColor}">${formattedNet}</span>
                </div>
            </div>
        </div>
    `;
}
