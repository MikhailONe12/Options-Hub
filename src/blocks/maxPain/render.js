/**
 * Pure DOM renderer for MaxPain Block.
 * Implements Thin Block Contract v1.0.
 */
export function render(container, ctx) {
    if (!ctx.data) {
        container.innerHTML = '<div class="max-pain-block">No Max Pain Data</div>';
        return;
    }

    const {
        formattedMaxPain,
        formattedPrice,
        formattedDistance,
        pct,
        color,
        conclusion,
        distance
    } = ctx.data;

    const arrow = distance < 0 ? '▲' : '▼';

    container.innerHTML = `
        <div class="max-pain-block max-pain--${color}">
            <div class="block-title">MAX PAIN</div>
            
            <div class="max-pain-content">
                <div class="max-pain-value">
                    <div class="max-pain-value__price">$${formattedMaxPain}</div>
                    <div class="max-pain-value__label">Max Pain Strike</div>
                </div>

                <div class="max-pain-spot">
                    <div class="max-pain-spot__label">CURRENT PRICE</div>
                    <div class="max-pain-spot__price">$${formattedPrice}</div>
                </div>

                <div class="max-pain-distance">
                    <div class="max-pain-indicator"></div>
                    <div>
                        <div class="max-pain-distance__text">
                            ${arrow} $${formattedDistance} (${distance > 0 ? '+' : ''}${pct.toFixed(2)}%)
                        </div>
                        <div class="max-pain-distance__label">Distance to Max Pain</div>
                    </div>
                </div>

                <div class="max-pain-conclusion">
                    <div style="font-size: 10px; line-height: 1.4; font-weight: 500;">${conclusion}</div>
                </div>
            </div>
        </div>
    `;
}
