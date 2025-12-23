/**
 * Pure DOM renderer for Universal Metric Block.
 * Implements Thin Block Contract v1.0.
 */
export function render(container, ctx) {
    if (!ctx.data) {
        container.innerHTML = '<div class="metric-card metric-card--empty">No Data</div>';
        return;
    }

    const { label, formatted, delta, color } = ctx.data;
    const { theme } = ctx.ui;

    const deltaHtml = delta !== undefined
        ? `<div class="metric-card__delta">${delta >= 0 ? '+' : ''}${delta}%</div>`
        : '';

    container.innerHTML = `
        <div class="metric-card metric-card--${color} metric-card--${theme}">
            <div class="metric-card__label">${label}</div>
            <div class="metric-card__value">${formatted}</div>
            ${deltaHtml}
        </div>
    `;
}
