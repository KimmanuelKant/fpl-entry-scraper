// fpl-scraper.js
// A script to scrape Fantasy Premier League (FPL) team data.
// Fetches information about teams and managers in configurable chunks.

const fs = require('fs').promises;
const https = require('https');

// Configure connection pooling for better performance and reliability
const agent = new https.Agent({
    keepAlive: true,           // Keep connections alive for reuse
    maxSockets: 25,            // Maximum number of concurrent connections
    timeout: 60000,            // Socket timeout (60 seconds)
    maxFreeSockets: 10         // Maximum number of idle sockets to keep
});

// Utility function for adding delays
async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Fetches data for a single team with retry logic for rate limiting
 * @param {string} url - The API endpoint URL
 * @param {number} maxRetries - Maximum number of retry attempts
 * @returns {Promise<Object>} - The team data
 */
async function fetchWithRetry(url, maxRetries = 3) {
    let lastError;
    let retryCount = 0;

    while (retryCount < maxRetries) {
        try {
            // Set up request with timeout
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 20000);
            
            const response = await fetch(url, { 
                agent,
                signal: controller.signal,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
            });
            clearTimeout(timeoutId);

            // Handle rate limiting with exponential backoff
            if (response.status === 429) {
                const backoffTime = Math.pow(2, retryCount) * 1000 + Math.random() * 1000;
                console.log(`Rate limited, waiting ${(backoffTime/1000).toFixed(1)}s before retry ${retryCount + 1}/${maxRetries}`);
                await sleep(backoffTime);
                retryCount++;
                continue;
            }

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            return await response.json();

        } catch (error) {
            lastError = error;
            if (error.name === 'AbortError') {
                throw new Error('Request timed out');
            }
            if (retryCount === maxRetries - 1) {
                throw error;
            }
            retryCount++;
        }
    }
    
    throw lastError;
}

/**
 * Saves a chunk of teams to JSON files with error information
 * @param {Array} teams - Teams in current chunk
 * @param {Array} failedIds - IDs that failed in current chunk
 * @param {Map} failureDetails - Error details for failed IDs
 * @param {number} chunkStart - Starting ID of this chunk
 * @param {number} chunkEnd - Ending ID of this chunk
 */
async function saveProgress(teams, failedIds, failureDetails, chunkStart, chunkEnd) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    
    // Save successful teams for this chunk
    await fs.writeFile(
        `fpl_teams_${chunkStart}-${chunkEnd}_${timestamp}.json`,
        JSON.stringify(teams, null, 2)
    );
    
    // Save failed IDs and their error details
    await fs.writeFile(
        `failed_ids_${chunkStart}-${chunkEnd}_${timestamp}.json`,
        JSON.stringify({
            ids: failedIds,
            details: Object.fromEntries(failureDetails)
        }, null, 2)
    );
    
    console.log(`Chunk ${chunkStart}-${chunkEnd} saved`);
}

/**
 * Main scraping function that processes teams in batches
 * @param {number} startId - First team ID to scrape
 * @param {number} endId - Last team ID to scrape
 * @param {number} batchSize - Number of concurrent requests
 * @returns {Promise<Object>} - Results including teams and failure information
 */
async function scrapeTeams(startId, endId, batchSize = 100) {
    // Initialize chunk tracking
    let currentChunkTeams = [];
    let currentChunkFailedIds = [];
    const failureDetails = new Map();
    const startTime = Date.now();
    let consecutiveFailures = 0;
    
    const CHUNK_SIZE = 50000; // Save data every 50k teams
    let chunkStartId = startId;

    // Process teams in batches
    for (let id = startId; id <= endId; id += batchSize) {
        const batchPromises = [];
        const currentBatchEnd = Math.min(id + batchSize - 1, endId);

        // Create batch of promises for concurrent requests
        for (let batchId = id; batchId <= currentBatchEnd; batchId++) {
            batchPromises.push(
                fetchWithRetry(`https://fantasy.premierleague.com/api/entry/${batchId}/`)
                    .then(result => {
                        consecutiveFailures = 0;
                        return {
                            id: batchId,
                            player_first_name: result.player_first_name,
                            player_last_name: result.player_last_name,
                            team_name: result.name,
                            entry: result.entry
                        };
                    })
                    .catch(err => {
                        consecutiveFailures++;
                        currentChunkFailedIds.push(batchId);
                        failureDetails.set(batchId, err.message);
                        
                        // Increase delay if too many failures
                        if (consecutiveFailures > 5) {
                            console.log('Too many consecutive failures, increasing delay...');
                            return sleep(consecutiveFailures * 1000).then(() => null);
                        }
                        return null;
                    })
            );
        }

        // Wait for batch completion and process results
        const batchResults = await Promise.all(batchPromises);
        const validTeams = batchResults.filter(result => result !== null);
        currentChunkTeams.push(...validTeams);

        // Log progress
        const timeElapsed = (Date.now() - startTime) / 1000;
        const teamsPerSecond = currentChunkTeams.length / timeElapsed;
        
        console.log(`
Progress Update:
---------------
Current ID Range: ${id} - ${currentBatchEnd}
Teams in Current Chunk: ${currentChunkTeams.length}
Failed IDs in Current Chunk: ${currentChunkFailedIds.length}
Time Elapsed: ${timeElapsed.toFixed(2)}s
Rate: ${teamsPerSecond.toFixed(2)} teams/s

Recent Failures:
${Array.from(failureDetails.entries())
    .slice(-5)
    .map(([id, error]) => `ID ${id}: ${error}`)
    .join('\n')}
        `);

        // Save data when chunk is complete
        if (currentBatchEnd >= chunkStartId + CHUNK_SIZE - 1 || currentBatchEnd >= endId) {
            await saveProgress(
                currentChunkTeams, 
                currentChunkFailedIds, 
                new Map([...failureDetails].filter(([id]) => id >= chunkStartId && id <= currentBatchEnd)),
                chunkStartId,
                currentBatchEnd
            );
            
            // Reset chunk tracking
            currentChunkTeams = [];
            currentChunkFailedIds = [];
            failureDetails.clear();
            chunkStartId = currentBatchEnd + 1;
        }

        // Dynamic delay between batches based on failure rate
        const batchDelay = Math.min(100 + (consecutiveFailures * 50), 1000);
        await sleep(batchDelay);
    }

    return {
        totalTeams: currentChunkTeams,
        totalFailedIds: currentChunkFailedIds,
        failureDetails
    };
}

/**
 * Main execution function
 * Sets up the scraping process and handles final results
 */
async function main() {
    try {
        // Configuration
        const START_ID = 1;
        const END_ID = 11138102;    // Adjust this number for different ranges
        const BATCH_SIZE = 100;   // Number of concurrent requests
        
        console.log('Starting FPL team scraper...');
        console.log(`Scraping teams from ID ${START_ID} to ${END_ID}`);
        console.log(`Batch size: ${BATCH_SIZE}`);
        
        const startTime = Date.now();
        const result = await scrapeTeams(START_ID, END_ID, BATCH_SIZE);
        const timeElapsed = (Date.now() - startTime) / 1000;

        // Calculate and log error statistics
        const errorCounts = new Map();
        result.failureDetails.forEach((error) => {
            errorCounts.set(error, (errorCounts.get(error) || 0) + 1);
        });

        // Log final summary
        console.log(`
Scraping Complete!
-----------------
Total Teams Found: ${result.totalTeams.length}
Failed IDs: ${result.totalFailedIds.length}
Time Elapsed: ${timeElapsed.toFixed(2)}s
Average Rate: ${(result.totalTeams.length / timeElapsed).toFixed(2)} teams/s

Error Patterns:
${Array.from(errorCounts.entries())
    .map(([error, count]) => `${error}: ${count} occurrences`)
    .join('\n')}
        `);

    } catch (error) {
        console.error('Fatal error during scraping:', error);
    }
}

// Start the scraping process
main().catch(console.error);