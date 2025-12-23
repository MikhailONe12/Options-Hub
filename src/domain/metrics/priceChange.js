/**
 * Price Change Calculator
 * 
 * Contract: Calculates percentage price change over a period.
 * - priceHistory: Array of numbers (prices in ascending time order)
 * - Returns: Percentage change, e.g., 2.5 for +2.5%
 * - Returns 0 if: insufficient data, invalid data, or first price <= 0
 * 
 * Note: changePct === 0 may indicate either no data OR actual 0% change.
 * Distinguish via: priceHistory.length < 2
 */
export function calculatePriceChange(priceHistory) {
    // Validate input
    if (!Array.isArray(priceHistory) || priceHistory.length < 2) {
        return 0;
    }

    // Handle both pure numbers and objects with .price property
    const first = typeof priceHistory[0] === 'number' 
        ? priceHistory[0] 
        : priceHistory[0]?.price;
    
    const last = typeof priceHistory[priceHistory.length - 1] === 'number'
        ? priceHistory[priceHistory.length - 1]
        : priceHistory[priceHistory.length - 1]?.price;

    // Strict validation: reject NaN, Infinity, -Infinity
    if (!Number.isFinite(first) || !Number.isFinite(last)) {
        return 0;
    }

    // Protect against division by zero or negative base
    if (first <= 0) {
        return 0;
    }

    const change = ((last - first) / first) * 100;
    
    // Final safety: ensure result is a valid number
    return Number.isFinite(change) ? change : 0;
}
