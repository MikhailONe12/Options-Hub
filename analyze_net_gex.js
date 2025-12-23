/**
 * Script to analyze Net GEX distribution and check for negative values
 */

async function analyzeNetGexDistribution() {
    try {
        const response = await fetch('http://localhost:9000/api/btc/gex/detailed-strikes');

        if (!response.ok) {
            console.error('HTTP error:', response.status, response.statusText);
            return;
        }

        const data = await response.json();
        console.log('Total strikes loaded:', data.strikes?.length || 0);

        if (!data.strikes || data.strikes.length === 0) {
            console.log('No strike data available');
            return;
        }

        // Analyze distribution
        const strikes = data.strikes;

        const positiveGex = strikes.filter(s => s.net_gex > 0);
        const negativeGex = strikes.filter(s => s.net_gex < 0);
        const neutralGex = strikes.filter(s => s.net_gex === 0);

        console.log('\n========================================');
        console.log('NET GEX DISTRIBUTION ANALYSIS');
        console.log('========================================');
        console.log(`Total strikes: ${strikes.length}`);
        console.log(`Positive Net GEX: ${positiveGex.length} (${(positiveGex.length / strikes.length * 100).toFixed(1)}%)`);
        console.log(`Negative Net GEX: ${negativeGex.length} (${(negativeGex.length / strikes.length * 100).toFixed(1)}%)`);
        console.log(`Neutral Net GEX: ${neutralGex.length} (${(neutralGex.length / strikes.length * 100).toFixed(1)}%)`);

        // Find min and max
        const maxPositive = Math.max(...strikes.map(s => s.net_gex));
        const maxNegative = Math.min(...strikes.map(s => s.net_gex));

        console.log('\n--- Range ---');
        console.log(`Max positive Net GEX: ${maxPositive.toFixed(2)}`);
        console.log(`Max negative Net GEX: ${maxNegative.toFixed(2)}`);
        console.log(`Total range: ${(maxPositive - maxNegative).toFixed(2)}`);

        // Find strikes with max values
        const maxPosStrike = strikes.find(s => s.net_gex === maxPositive);
        const maxNegStrike = strikes.find(s => s.net_gex === maxNegative);

        console.log('\n--- Extreme Values ---');
        console.log(`Strike with MAX positive Net GEX: ${maxPosStrike.strike} (${maxPosStrike.net_gex.toFixed(2)})`);
        console.log(`  Call GEX: ${maxPosStrike.call_gex.toFixed(2)}, Put GEX: ${maxPosStrike.put_gex.toFixed(2)}`);
        console.log(`Strike with MAX negative Net GEX: ${maxNegStrike.strike} (${maxNegStrike.net_gex.toFixed(2)})`);
        console.log(`  Call GEX: ${maxNegStrike.call_gex.toFixed(2)}, Put GEX: ${maxNegStrike.put_gex.toFixed(2)}`);

        // Show top 10 most negative
        console.log('\n--- Top 10 Most NEGATIVE Net GEX ---');
        const topNegative = strikes
            .filter(s => s.net_gex < 0)
            .sort((a, b) => a.net_gex - b.net_gex)
            .slice(0, 10);

        topNegative.forEach((s, i) => {
            console.log(`${i + 1}. Strike ${s.strike}: Net GEX = ${s.net_gex.toFixed(2)} (Call: ${s.call_gex.toFixed(2)}, Put: ${s.put_gex.toFixed(2)})`);
        });

        // Show top 10 most positive
        console.log('\n--- Top 10 Most POSITIVE Net GEX ---');
        const topPositive = strikes
            .filter(s => s.net_gex > 0)
            .sort((a, b) => b.net_gex - a.net_gex)
            .slice(0, 10);

        topPositive.forEach((s, i) => {
            console.log(`${i + 1}. Strike ${s.strike}: Net GEX = ${s.net_gex.toFixed(2)} (Call: ${s.call_gex.toFixed(2)}, Put: ${s.put_gex.toFixed(2)})`);
        });

        // Check for magnitude issues
        console.log('\n--- Magnitude Analysis ---');
        const avgAbsGex = strikes.reduce((sum, s) => sum + Math.abs(s.net_gex), 0) / strikes.length;
        console.log(`Average absolute Net GEX: ${avgAbsGex.toFixed(2)}`);

        // Check if negative values are too small to see
        const significantNegative = negativeGex.filter(s => Math.abs(s.net_gex) > 1);
        console.log(`Negative Net GEX with |value| > 1: ${significantNegative.length}`);

        if (significantNegative.length > 0) {
            console.log('\nSignificant negative values:');
            significantNegative.slice(0, 5).forEach(s => {
                console.log(`  Strike ${s.strike}: ${s.net_gex.toFixed(2)}`);
            });
        }

        // Check the calculation formula
        console.log('\n--- Formula Verification (sample) ---');
        const sample = strikes.slice(0, 5);
        sample.forEach(s => {
            const calculated = s.call_gex - s.put_gex;
            const fromServer = s.net_gex;
            const match = Math.abs(calculated - fromServer) < 0.01;
            console.log(`Strike ${s.strike}:`);
            console.log(`  Call GEX: ${s.call_gex.toFixed(2)}, Put GEX: ${s.put_gex.toFixed(2)}`);
            console.log(`  Calculated Net GEX: ${calculated.toFixed(2)}`);
            console.log(`  Server Net GEX: ${fromServer.toFixed(2)}`);
            console.log(`  Match: ${match ? '✓' : '✗'}`);
        });

    } catch (e) {
        console.error('Error:', e.message);
    }
}

analyzeNetGexDistribution();
