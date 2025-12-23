/**
 * Pure DOM renderer for Greeks Block.
 * Implements Thin Block Contract v1.0.
 */
export function render(container, ctx) {
    if (!ctx.data) {
        container.innerHTML = '<div class="greeks-block">No Greek Data</div>';
        return;
    }

    const { firstOrder, secondOrder } = ctx.data;

    const firstOrderHtml = firstOrder.map(g => `
        <div class="greek-item">
            <span class="greek-item__label greek--${g.color}">${g.label}</span>
            <span class="greek-item__value">${g.formatted}</span>
        </div>
    `).join('');

    const secondOrderHtml = secondOrder.map(g => `
        <div class="greek-item">
            <span class="greek-item__label" style="opacity: 0.8">${g.label}</span>
            <span class="greek-item__value" style="font-size: 13px">${g.formatted}</span>
        </div>
    `).join('');

    container.innerHTML = `
        <div class="greeks-block">
            <div class="block-title">GREEKS</div>
            <div class="greeks-grid">
                <div class="greek-section">
                    <div class="greek-section__title">First Order</div>
                    <div class="greek-list">${firstOrderHtml}</div>
                </div>
                <div class="greek-section">
                    <div class="greek-section__title">Second Order</div>
                    <div class="greek-list">${secondOrderHtml}</div>
                </div>
            </div>
        </div>
    `;
}
