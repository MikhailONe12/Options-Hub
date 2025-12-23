/**
 * Blocks Registry Entry Point
 * Imports all blocks to trigger their registration side-effects.
 */

// Core Logic Blocks
import './gexMatrix/index.js';
import './gexTable/index.js';

// Metric Blocks
import './greeks/index.js';
import './maxPain/index.js';
import './pcRatio/index.js';
import './volatilitySmile/index.js';
import './priceComparison/index.js';

// Universal Blocks
import './metric/index.js';

console.log('[Blocks] All blocks registered.');
