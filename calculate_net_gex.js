/**
 * Standalone NET GEX Calculator
 * Replicates the data fetching and calculations from GEX LIVE - MATRIX block
 */

// Configuration - can be overridden by command line or environment
const CONFIG = {
  // Default asset to analyze
  asset: process.env.ASSET || 'btc',
  // Server base URL (adjust as needed)
  serverBaseUrl: process.env.SERVER_BASE_URL || 'http://localhost:9000'
};

/**
 * Format numbers similar to the GEX Matrix block
 */
function fmt(v, decimals = 0) {
  if (v == null || isNaN(v)) return '--';
  const absV = Math.abs(v);
  if (absV >= 1e9) return (v / 1e9).toFixed(1) + 'B';
  if (absV >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (absV >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return Number(v).toFixed(decimals);
}

/**
 * Format strike prices without abbreviations
 */
function fmtStrike(v) {
  if (v == null || isNaN(v)) return '--';
  return Number(v).toLocaleString('ru-RU', { maximumFractionDigits: 0 });
}

/**
 * Load expiration dates for the asset
 */
async function loadExpirationDates(asset) {
  try {
    const url = `${CONFIG.serverBaseUrl}/api/${asset}/gex/expirations`;
    console.log('[NET GEX Calculator] Loading expirations from:', url);
    const response = await fetch(url);
    if (!response.ok) {
      console.error('[NET GEX Calculator] HTTP error loading expirations:', response.status, response.statusText);
      return [];
    }
    const data = await response.json();
    console.log('[NET GEX Calculator] Loaded expirations:', data.expirations?.length || 0);
    // Extract just the expiration dates
    return data.expirations?.map(item => item.expiration_date) || [];
  } catch (e) {
    console.error('[NET GEX Calculator] Error loading expirations:', e.message || e);
    console.error('[NET GEX Calculator] Make sure the server is running at:', CONFIG.serverBaseUrl);
    return [];
  }
}

/**
 * Load strikes data for the asset and dates (using detailed endpoint)
 */
async function loadStrikes(asset, dates, mode = 'all') {
  try {
    // Use the detailed endpoint that provides separate call_gex and put_gex values
    const url = `${CONFIG.serverBaseUrl}/api/${asset}/gex/detailed-strikes`;
    console.log('[NET GEX Calculator] Loading detailed strikes from:', url);
    const response = await fetch(url);
    if (!response.ok) {
      console.error('[NET GEX Calculator] HTTP error loading strikes:', response.status, response.statusText);
      return [];
    }
    const data = await response.json();
    console.log('[NET GEX Calculator] Loaded strikes:', data.strikes?.length || 0);
    
    // Debug: Log the structure of the first few strike data items
    if (data.strikes && data.strikes.length > 0) {
      console.log('[NET GEX Calculator] Sample strike data structure:', JSON.stringify(data.strikes.slice(0, 3), null, 2));
    }
    
    return data.strikes || [];
  } catch (e) {
    console.error('[NET GEX Calculator] Error loading strikes:', e.message || e);
    console.error('[NET GEX Calculator] Make sure the server is running at:', CONFIG.serverBaseUrl);
    return [];
  }
}

/**
 * Load current price for the asset
 */
async function loadCurrentPrice(asset) {
  try {
    const url = `${CONFIG.serverBaseUrl}/api/${asset}-metrics/latest`;
    console.log('[NET GEX Calculator] Loading price from:', url);
    const response = await fetch(url);
    if (!response.ok) {
      console.error('[NET GEX Calculator] HTTP error loading price:', response.status, response.statusText);
      return 0;
    }
    const data = await response.json();
    const price = data.underlying_price || data.spotPrice || 0;
    console.log('[NET GEX Calculator] Loaded price:', price);
    return price;
  } catch (e) {
    console.error('[NET GEX Calculator] Error loading price:', e.message || e);
    console.error('[NET GEX Calculator] Make sure the server is running at:', CONFIG.serverBaseUrl);
    return 0;
  }
}

/**
 * Calculate and display NET GEX data
 */
async function calculateAndDisplayNetGex(asset = CONFIG.asset) {
  console.log(`=====================================`);
  console.log(`NET GEX CALCULATOR FOR ASSET: ${asset.toUpperCase()}`);
  console.log(`Server URL: ${CONFIG.serverBaseUrl}`);
  console.log(`=====================================`);
  
  try {
    // Load current price
    const currentPrice = await loadCurrentPrice(asset);
    console.log(`Current Price: ${currentPrice}`);
    
    // Load expiration dates
    const expirations = await loadExpirationDates(asset);
    if (!expirations.length) {
      console.log("No expiration dates available - server may be unreachable or no data available");
      console.log("Please check that:");
      console.log("1. The server is running");
      console.log("2. The server URL is correct in the CONFIG section");
      console.log("3. The asset is valid (btc, eth, sol)");
      return;
    }
    
    // Load strikes data (the detailed endpoint gets data for multiple expirations automatically)
    const strikes = await loadStrikes(asset, expirations);
    if (!strikes.length) {
      console.log("No strike data available - server may be unreachable or no data available");
      return;
    }
    
    console.log(`\nNET GEX Data for Each Strike:`);
    console.log(`=====================================`);
    
    let totalCallGex = 0;
    let totalPutGex = 0;
    let callStrikeCount = 0;
    let putStrikeCount = 0;
    
    // Sort strikes by strike price for better display
    const sortedStrikes = strikes.sort((a, b) => a.strike - b.strike);
    
    sortedStrikes.forEach((strikeData, index) => {
      const strike = strikeData.strike;
      
      // Extract GEX values from the detailed data structure
      const callGex = strikeData.call_gex || 0;
      const putGex = strikeData.put_gex || 0;
      // For net GEX, we follow the specification: call gamma contributes positively, put gamma contributes negatively
      const netGex = callGex + (-Math.abs(putGex)); // Put GEX is negative
      
      // Determine which side is dominant
      let dominantSide = "Neutral";
      if (callGex > Math.abs(putGex)) {
        dominantSide = "Call";
        totalCallGex += callGex;
        callStrikeCount++;
      } else if (Math.abs(putGex) > callGex) {
        dominantSide = "Put";
        // For put side, we're counting the absolute value since put_gex is typically negative
        totalPutGex += Math.abs(putGex);
        putStrikeCount++;
      }
      
      console.log(`Strike ${fmtStrike(strike)}: NET GEX = ${fmt(netGex, 2)} (Call GEX: ${fmt(callGex, 2)}, Put GEX: ${fmt(-Math.abs(putGex), 2)}) - Dominant: ${dominantSide}`);
    });
    
    console.log(`\n=====================================`);
    console.log(`SUMMARY FOR ${asset.toUpperCase()}:`);
    console.log(`=====================================`);
    console.log(`Analyzing ${sortedStrikes.length} strike levels`);
    console.log(`Current Price: ${fmt(currentPrice, 2)}`);
    console.log(`Total Call GEX: ${fmt(totalCallGex, 2)} across ${callStrikeCount} strikes`);
    console.log(`Total Put GEX: ${fmt(totalPutGex, 2)} across ${putStrikeCount} strikes`);
    
    // Calculate net totals (Call positive, Put negative)
    const netTotal = totalCallGex - totalPutGex;
    
    if (netTotal > 0) {
      console.log(`Overall Dominance: Call side is stronger by ${fmt(netTotal, 2)}`);
    } else if (netTotal < 0) {
      console.log(`Overall Dominance: Put side is stronger by ${fmt(Math.abs(netTotal), 2)}`);
    } else {
      console.log(`Overall Dominance: Balanced between Call and Put`);
    }
    
    // Find key levels
    let maxCallGexStrike = null;
    let maxCallGexValue = -Infinity;
    let maxPutGexStrike = null;
    let maxPutGexValue = 0; // Changed to 0 to properly track negative values
    
    sortedStrikes.forEach(strikeData => {
      const callGex = strikeData.call_gex || 0;
      const putGex = strikeData.put_gex || 0;
      
      if (callGex > maxCallGexValue) {
        maxCallGexValue = callGex;
        maxCallGexStrike = strikeData.strike;
      }
      
      // For put GEX, we want the most negative value (largest in absolute terms)
      // Since putGex is already negative, we're looking for the smallest value
      if (putGex < maxPutGexValue) {
        maxPutGexValue = putGex;
        maxPutGexStrike = strikeData.strike;
      }
    });
    
    console.log(`\nKEY LEVELS:`);
    if (maxCallGexStrike && maxCallGexValue > 0) {
      console.log(`Strongest Call GEX: Strike ${fmtStrike(maxCallGexStrike)} with value ${fmt(maxCallGexValue, 2)}`);
    }
    if (maxPutGexStrike && maxPutGexValue < 0) {
      console.log(`Strongest Put GEX: Strike ${fmtStrike(maxPutGexStrike)} with value ${fmt(maxPutGexValue, 2)}`);
    }
    
    // Find closest strike to current price
    if (currentPrice > 0) {
      let closestStrike = sortedStrikes[0].strike;
      let minDiff = Math.abs(sortedStrikes[0].strike - currentPrice);
      
      for (let i = 1; i < sortedStrikes.length; i++) {
        const diff = Math.abs(sortedStrikes[i].strike - currentPrice);
        if (diff < minDiff) {
          minDiff = diff;
          closestStrike = sortedStrikes[i].strike;
        }
      }
      
      console.log(`\nPRICE ANALYSIS:`);
      console.log(`Current Price: ${fmt(currentPrice, 2)}`);
      console.log(`Closest Strike: ${fmtStrike(closestStrike)}`);
      console.log(`Distance: ${fmt(minDiff, 2)} (${((minDiff / currentPrice) * 100).toFixed(2)}%)`);
    }
    
  } catch (e) {
    console.error('[NET GEX Calculator] Error in calculation:', e.message || e);
  }
}

// Expose function globally
if (typeof window !== 'undefined') {
  window.calculateAndDisplayNetGex = calculateAndDisplayNetGex;
  console.log("[NET GEX Calculator] Loaded. Use calculateAndDisplayNetGex('asset') to analyze GEX data.");
  console.log("[NET GEX Calculator] Example: calculateAndDisplayNetGex('btc')");
}

// For Node.js environment
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { calculateAndDisplayNetGex, CONFIG };
}

// Auto-run for BTC if in Node.js environment
if (typeof require !== 'undefined' && require.main === module) {
  // This is the main module, so run the calculation
  calculateAndDisplayNetGex(CONFIG.asset).then(() => {
    console.log('\nNET GEX calculation completed.');
  }).catch((error) => {
    console.error('Error running NET GEX calculation:', error.message || error);
  });
}