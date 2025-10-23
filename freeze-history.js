// freeze-history.js - OPTIMIZED VERSION
// 1. Import necessary packages
require('dotenv').config();
const axios = require('axios');
const { createObjectCsvWriter } = require('csv-writer');
const path = require('path');
const fs = require('fs');
const { google } = require('googleapis');

// 2. Configuration and Constants - OPTIMIZED
const MOMENCE_ALL_COOKIES = process.env.MOMENCE_ALL_COOKIES;
const BATCH_SIZE = 50; // Reduced from 200 to 50 to handle severe rate limiting
const CONCURRENT_BATCHES = 2; // Reduced from 4 to 2 to handle severe rate limiting
const OUTPUT_CSV_PATH = path.join(__dirname, 'freezes.csv');
const OUTPUT_JSON_PATH = path.join(__dirname, 'data.json');
const POLLING_INTERVAL_MS = 5000; // Reduced from 10000
const MAX_POLLING_ATTEMPTS = 100;
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 2000; // Increased to handle rate limiting
const RATE_LIMIT_DELAY_MS = 5000; // Increased from 5000 to 8000 for more aggressive rate limit handling

// Google Sheets Configuration
const SPREADSHEET_ID = '1ohcf0GrsKD-3m0yVv2-05l1oNjlM-A0Uqo5dCjQXtCg';
const CHECKINS_SPREADSHEET_ID = '149ILDqovzZA6FRUJKOwzutWdVqmqWBtWPfzG3A0zxTI';
const SHEET_NAME = 'Freezes';
const CHECKINS_SHEET_NAME = 'Checkins';
const SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];

// Google OAuth Configuration
const GOOGLE_OAUTH = {
    CLIENT_ID: process.env.GOOGLE_CLIENT_ID,
    CLIENT_SECRET: process.env.GOOGLE_CLIENT_SECRET,
    REFRESH_TOKEN: process.env.GOOGLE_REFRESH_TOKEN,        
    TOKEN_URL: "https://oauth2.googleapis.com/token"
};

if (!MOMENCE_ALL_COOKIES) {
    console.error("FATAL ERROR: MOMENCE_ALL_COOKIES is not defined in the .env file.");
    process.exit(1);
}

// Validate Google OAuth environment variables
if (!GOOGLE_OAUTH.CLIENT_ID || !GOOGLE_OAUTH.CLIENT_SECRET || !GOOGLE_OAUTH.REFRESH_TOKEN) {
    console.error("FATAL ERROR: Google OAuth credentials are not properly configured in the .env file.");
    console.error("Required variables: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN");
    process.exit(1);
}

// Axios instance with optimizations
const momenceApi = axios.create({
    headers: {
        'Accept': 'application/json',
        'Cookie': MOMENCE_ALL_COOKIES,
        'Content-Type': 'application/json',
    },
    timeout: 30000, // 30 second timeout
    maxRedirects: 5,
    // Connection pool optimizations
    maxSockets: 100,
    keepAlive: true
});

// NEW: Connection pool for better performance
const https = require('https');
const httpAgent = new https.Agent({
    keepAlive: true,
    maxSockets: 100,
    maxFreeSockets: 10,
    timeout: 30000,
    freeSocketTimeout: 30000,
});
momenceApi.defaults.httpsAgent = httpAgent;

// NEW: Cache for processed data to avoid reprocessing
const processedCache = new Map();

// NEW: Pre-compiled membership criteria for faster lookup
const membershipCriteriaMap = new Map([
    ['Studio 8 Class Package', { attempts: 1, days: 30 }],
    ['Studio 12 Class Package', { attempts: 1, days: 30 }],
    ['Studio 1 Month Unlimited Membership', { attempts: 1, days: 30 }],
    ['Studio 3 Month Unlimited Membership', { attempts: 3, days: 90 }],
    ['Studio 6 Month Unlimited Membership', { attempts: 6, days: 180 }],
    ['Studio Annual Unlimited Membership', { attempts: 12, days: 360 }],
    ['Studio 3 Month U/L Monthly Installment', { attempts: 1, days: 30 }],
    ['Studio 20 Single Class Pack', { attempts: 3, days: 90 }],
    ['Studio 10 Single Class Pack', { attempts: 2, days: 60 }],
    ['Studio 30 Single Class Pack', { attempts: 3, days: 90 }],
    ['Limited Edition : 57 Class Pack', { attempts: 6, days: 180 }],
    ['VIP ALL ACCESS - Studio 1 Month Unlimited Membership', { attempts: 1, days: 30 }],
    ['Studio Private Class X 10', { attempts: 1, days: 30 }],
    ['V\'Day Special: Shared Studio 20 Single Class', { attempts: 3, days: 90 }],
    ['V\'Day Special: Shared Studio 8 Class Package', { attempts: 1, days: 30 }],
    ['Studio 30 Private Class Package', { attempts: 3, days: 90 }]
]);

// --- Retry Function - OPTIMIZED ---
async function retryApiCall(url, memberId, hostId, attempts = 0) {
    try {
        const response = await momenceApi.get(url);
        return response;
    } catch (error) {
        // Enhanced error logging
        const errorDetails = {
            memberId,
            hostId,
            url,
            status: error.response?.status,
            statusText: error.response?.statusText,
            message: error.message,
            attempt: attempts + 1
        };
        
        // Handle rate limiting (429) with special delay
        if (error.response?.status === 429) {
            if (attempts < MAX_RETRY_ATTEMPTS) {
                const delay = RATE_LIMIT_DELAY_MS * Math.pow(2, attempts); // Exponential backoff for rate limiting
                console.warn(`⏳ Rate Limited - Retry ${attempts + 1}/${MAX_RETRY_ATTEMPTS} for member ${memberId} (host ${hostId}) after ${delay}ms`);
                await new Promise(resolve => setTimeout(resolve, delay));
                return retryApiCall(url, memberId, hostId, attempts + 1);
            }
        }
        // Handle server errors (5xx) with regular retry
        else if (attempts < MAX_RETRY_ATTEMPTS && error.response?.status >= 500) {
            const delay = RETRY_DELAY_MS * Math.pow(1.5, attempts); // Reduced backoff multiplier
            console.warn(`🔄 Retry ${attempts + 1}/${MAX_RETRY_ATTEMPTS} for member ${memberId} (host ${hostId}) - Status: ${error.response?.status} - ${error.response?.statusText || error.message}`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return retryApiCall(url, memberId, hostId, attempts + 1);
        } else {
            // Log all failed requests for debugging
            console.error(`❌ FAILED REQUEST: Member ${memberId}, Host ${hostId}, Status: ${error.response?.status || 'Unknown'}, Error: ${error.message}`);
            throw error;
        }
    }
}

// --- Helper Functions - OPTIMIZED ---
const dateFormatter = new Intl.DateTimeFormat('en-IN', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
    timeZone: 'Asia/Kolkata'
});

function formatISTDate(dateString) {
    if (!dateString) return '-';
    try {
        const date = new Date(dateString);
        return dateFormatter.format(date).replace(',', '');
    } catch (e) { return '-'; }
}

// NEW: Batch format values for better performance
function formatValue(value) {
    return value === undefined || value === null || value === '' ? '-' : value;
}

function getPermittedValues(membershipName) {
    return membershipCriteriaMap.get(membershipName) || { attempts: 0, days: 0 };
}

// --- Google Sheets Data Extraction - OPTIMIZED ---
let authClient = null; // Cache auth client

async function getGoogleSheetsAuth() {
    if (authClient) return authClient;
    
    const oauth2Client = new google.auth.OAuth2(
        GOOGLE_OAUTH.CLIENT_ID,
        GOOGLE_OAUTH.CLIENT_SECRET,
        'urn:ietf:wg:oauth:2.0:oob'
    );

    oauth2Client.setCredentials({
        refresh_token: GOOGLE_OAUTH.REFRESH_TOKEN
    });

    authClient = oauth2Client;
    return oauth2Client;
}

async function fetchDataFromGoogleSheets() {
    console.log("Step 1: Fetching data from Google Sheets Checkins...");
    
    try {
        const auth = await getGoogleSheetsAuth();
        const sheets = google.sheets({ version: 'v4', auth });
        
        console.log("Fetching member data from Checkins sheet...");
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: CHECKINS_SPREADSHEET_ID,
            range: `${CHECKINS_SHEET_NAME}!A:W`,
        });

        const rows = response.data.values;
        if (!rows || rows.length === 0) {
            console.log(`No data found in the "${CHECKINS_SHEET_NAME}" sheet.`);
            console.log('FALLING BACK TO ORIGINAL ASYNC REPORTS METHOD...');
            return await fetchDataViaAsyncReports();
        }

        console.log(`Found ${rows.length} rows in Checkins sheet.`);
        
        // NEW: Use Set for faster deduplication
        const uniquePairs = new Set();
        
        // Skip header row and process in chunks for better memory usage
        for (let i = 1; i < rows.length; i++) {
            const row = rows[i];
            const memberId = row[0];
            const hostId = row[22];
            
            if (memberId && hostId) {
                uniquePairs.add(`${memberId}:${hostId}`);
            }
        }

        const memberHostPairs = Array.from(uniquePairs).map(pair => {
            const [memberId, hostId] = pair.split(':');
            return { memberId: parseInt(memberId), hostId: hostId.toString() };
        });

        console.log(`Found ${memberHostPairs.length} unique pairs from Checkins sheet.`);
        return { memberHostPairs };
    } catch (error) {
        console.error('Error fetching data from Google Sheets:', error.message);
        console.log('FALLING BACK TO ORIGINAL ASYNC REPORTS METHOD...');
        return await fetchDataViaAsyncReports();
    }
}

// --- Async Reports (unchanged but with minor optimizations) ---
async function pollForReportCompletion(hostId, reportRunId) {
    const url = `https://api.momence.com/host/${hostId}/reports/session-bookings/report-runs/${reportRunId}`;
    let attempts = 0;

    while (attempts < MAX_POLLING_ATTEMPTS) {
        try {
            const response = await momenceApi.get(url);
            const reportStatus = response.data;

            const isCompleted =
                reportStatus.status?.toUpperCase() === 'COMPLETED' ||
                reportStatus.state?.toUpperCase() === 'COMPLETED' ||
                reportStatus.status?.toUpperCase() === 'SUCCESS' ||
                reportStatus.state?.toUpperCase() === 'FINISHED' ||
                reportStatus.completed === true ||
                reportStatus.isCompleted === true ||
                (reportStatus.data && reportStatus.data.items && Array.isArray(reportStatus.data.items));

            const isFailed =
                reportStatus.status?.toUpperCase() === 'FAILED' ||
                reportStatus.state?.toUpperCase() === 'FAILED' ||
                reportStatus.status?.toUpperCase() === 'ERROR' ||
                reportStatus.error === true ||
                reportStatus.failed === true;

            if (isCompleted) {
                console.log(` -> Report for host ${hostId} (ID: ${reportRunId}) is COMPLETED.`);
                return reportStatus;
            } else if (isFailed) {
                throw new Error(`Report run ${reportRunId} for host ${hostId} failed`);
            } else {
                const statusInfo = reportStatus.status || reportStatus.state || 'UNKNOWN';
                console.log(` -> Report for host ${hostId} (ID: ${reportRunId}) is ${statusInfo}. Waiting...`);
                await new Promise(resolve => setTimeout(resolve, POLLING_INTERVAL_MS));
            }
        } catch (error) {
            console.error(`Error polling for report ${reportRunId}:`, error.message);
            throw error;
        }
        attempts++;
    }
    throw new Error(`Report ${reportRunId} timed out after ${attempts} attempts.`);
}

async function fetchDataViaAsyncReports() {
    console.log("Step 1: Triggering asynchronous session booking reports...");
    const hosts = ['13752', '33905'];
    const payload = {
        "timeZone": "Asia/Kolkata",
        "startDate": "2025-07-01T18:30:00.000Z",
        "endDate": "2025-12-31T18:29:59.999Z",
        "startDate2": "2025-07-31T18:30:00.000Z",
        "endDate2": "2025-08-31T18:29:59.999Z",
        "day": "2025-08-26",
        "includeVatInRevenue": true,
        "computedSaleValue": true,
        "membershipTagIds": [],
        "sessionTagIds": [],
        "datePreset": 4,
        "datePreset2": 4
    };

    const reportJobs = await Promise.all(hosts.map(async (hostId) => {
        const url = `https://api.momence.com/host/${hostId}/reports/session-bookings/async`;
        try {
            const { data } = await momenceApi.post(url, payload);
            console.log(` -> Successfully triggered report for host ${hostId}. Report Run ID: ${data.reportRunId}`);
            return { hostId, reportRunId: data.reportRunId };
        } catch (error) {
            console.error(`Error triggering report for host ${hostId}: ${error.message}`);
            return null;
        }
    }));

    const validJobs = reportJobs.filter(job => job !== null);
    if (validJobs.length === 0) return { memberHostPairs: [] };

    console.log("\nStep 2: Polling for report completion...");
    const completedReports = await Promise.all(
        validJobs.map(job => pollForReportCompletion(job.hostId, job.reportRunId))
    );

    console.log("\nStep 3: Consolidating data from completed reports...");
    const uniquePairs = new Set();

    completedReports.forEach((report, index) => {
        const hostId = validJobs[index].hostId;
        if (report?.reportData?.items) {
            report.reportData.items.forEach(item => {
                if (item.memberId) {
                    uniquePairs.add(`${item.memberId}:${hostId}`);
                }
            });
        }
    });

    const memberHostPairs = Array.from(uniquePairs).map(pair => {
        const [memberId, hostId] = pair.split(':');
        return { memberId: parseInt(memberId), hostId: hostId.toString() };
    });

    console.log(`Found ${memberHostPairs.length} unique member/host pairs from async reports.`);
    return { memberHostPairs };
}

// --- NEW: Concurrent batch processor ---
async function processBatchConcurrently(batch, batchIndex) {
    const promises = batch.map(({ memberId, hostId }) =>
        retryApiCall(`https://api.momence.com/host/${hostId}/customers/${memberId}/history`, memberId, hostId)
        .catch(err => ({ error: err, memberId, hostId }))
    );

    const responses = await Promise.all(promises);
    const batchResults = [];
    let successCount = 0;
    let errorCount = 0;
    
    for (const response of responses) {
        // Handle undefined responses or responses with errors
        if (!response || response.error) {
            errorCount++;
            if (response && response.error) {
                console.warn(`❌ Batch ${batchIndex}: Failed for member ${response.memberId} (host ${response.hostId}): ${response.error.message}`);
            } else {
                console.warn(`❌ Batch ${batchIndex}: Undefined response received`);
            }
            continue;
        }

        successCount++;
        const historyEntries = response.data;
        if (!historyEntries || historyEntries.length === 0) continue;

        // NEW: Pre-calculate all data in one pass for better performance
        const memberData = processHistoryEntries(historyEntries);
        if (memberData && Array.isArray(memberData) && memberData.length > 0) {
            batchResults.push(...memberData);
        }
    }
    
    console.log(`✅ Batch ${batchIndex}: ${successCount} successful, ${errorCount} failed requests`);
    return batchResults;
}

// NEW: Optimized history processing function
function processHistoryEntries(historyEntries) {
    // Safety check
    if (!historyEntries || !Array.isArray(historyEntries)) {
        return [];
    }
    
    const sessionCounts = new Map();
    const locationData = new Map();
    const freezeData = new Map();
    const memberRows = [];

    // First pass: count sessions and extract location data
    for (const entry of historyEntries) {
        if (entry.type === 'session' && entry.boughtMembershipId) {
            const key = entry.boughtMembershipId;
            sessionCounts.set(key, (sessionCounts.get(key) || 0) + 1);
            
            if (entry.locationId && entry.locationName && !locationData.has(key)) {
                locationData.set(key, {
                    locationId: entry.locationId,
                    locationName: entry.locationName
                });
            }
        }
    }

    // Second pass: process memberships and freeze data
    for (const entry of historyEntries) {
        if (entry.type === 'membership') {
            const memberId = entry.memberId;
            const boughtMembershipId = entry.boughtMembershipId;

            const sessionsAttended = sessionCounts.get(boughtMembershipId) || 0;
            const sessionLocation = locationData.get(boughtMembershipId) || {};
            
            const rowData = {
                timestamp: formatISTDate(entry.timestamp),
                historyType: formatValue(entry.type),
                historyId: formatValue(entry.id),
                discountCode: formatValue(entry.discountCode),
                memberName: formatValue(entry.memberName),
                membershipName: formatValue(entry.membershipName),
                membershipId: formatValue(entry.membershipId),
                memberId: formatValue(memberId),
                boughtMembershipId: formatValue(boughtMembershipId),
                hostId: formatValue(entry.hostId),
                startDate: formatISTDate(entry.startDate),
                endDate: formatISTDate(entry.endDate),
                classesLeft: formatValue(entry.classesLeft),
                usageLimitForSessions: formatValue(entry.usageLimitForSessions),
                createdAt: formatISTDate(entry.createdAt),
                createdByUserId: formatValue(entry.createdByUserId),
                createdByUserName: formatValue(entry.createdByUserName),
                isFreezed: formatValue(entry.isFreezed),
                isVoided: formatValue(entry.isVoided),
                moneyLeft: formatValue(entry.moneyLeft),
                paymentTransactionId: formatValue(entry.paymentTransactionId),
                saleItemId: formatValue(entry.saleItemId),
                membershipType: formatValue(entry.membershipType),
                paymentMethod: formatValue(entry.paymentMethod),
                paymentSource: formatValue(entry.paymentSource),
                amountPaid: formatValue(entry.paid),
                sessionsAttended: sessionsAttended,
                locationId: formatValue(sessionLocation.locationId || entry.locationId),
                locationName: formatValue(sessionLocation.locationName || entry.locationName),
            };

            // Initialize freeze data
            const freezeKey = `${memberId}:${boughtMembershipId}`;
            if (!freezeData.has(freezeKey)) {
                freezeData.set(freezeKey, {
                    freezeAttempts: 0,
                    frozenDays: 0,
                    freezeDates: [],
                    freezeStartDate: null,
                    freezeEndDate: null
                });
            }

            // Process freeze activities
            if (entry.activities) {
                const freezeInfo = freezeData.get(freezeKey);
                for (const activity of entry.activities) {
                    if (activity.type === 'bought-membership-freezed') {
                        freezeInfo.freezeAttempts++;
                        freezeInfo.freezeDates.push({
                            type: 'freeze',
                            date: new Date(activity.createdAt)
                        });
                    } else if (activity.type === 'bought-membership-unfreezed') {
                        freezeInfo.freezeDates.push({
                            type: 'unfreeze',
                            date: new Date(activity.createdAt)
                        });
                    }
                }
            }

            memberRows.push(rowData);
        }
    }

    // Calculate freeze days and add to rows
    for (const row of memberRows) {
        const freezeKey = `${row.memberId}:${row.boughtMembershipId}`;
        const freezeInfo = freezeData.get(freezeKey) || { 
            freezeAttempts: 0, 
            frozenDays: 0, 
            freezeDates: [],
            freezeStartDate: null, 
            freezeEndDate: null 
        };

        // Calculate frozen days and extract all freeze attempt pairs
        let totalFrozenDays = 0;
        let freezeStartDate = null;
        let firstFreezeDate = null;
        let lastUnfreezeDate = null;
        const freezeAttemptPairs = [];

        freezeInfo.freezeDates.sort((a, b) => a.date - b.date);

        for (const event of freezeInfo.freezeDates) {
            if (event.type === 'freeze' && !freezeStartDate) {
                freezeStartDate = event.date;
                if (!firstFreezeDate) firstFreezeDate = event.date;
            } else if (event.type === 'unfreeze' && freezeStartDate) {
                const diffTime = Math.abs(event.date - freezeStartDate);
                totalFrozenDays += Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                lastUnfreezeDate = event.date;
                
                // Store this freeze attempt pair
                freezeAttemptPairs.push({
                    startDate: formatISTDate(freezeStartDate.toISOString()),
                    endDate: formatISTDate(event.date.toISOString())
                });
                
                freezeStartDate = null;
            }
        }

        // Handle ongoing freeze (no unfreeze yet)
        if (freezeStartDate) {
            const diffTime = Math.abs(new Date() - freezeStartDate);
            totalFrozenDays += Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            
            // Add ongoing freeze attempt
            freezeAttemptPairs.push({
                startDate: formatISTDate(freezeStartDate.toISOString()),
                endDate: 'Ongoing'
            });
        }

        const permittedValues = getPermittedValues(row.membershipName);
        const status = (freezeInfo.freezeAttempts > permittedValues.attempts || totalFrozenDays > permittedValues.days) ? 'Exceeded' : 'Within Limits';

        // Add freeze data to row
        row.freezeAttempts = freezeInfo.freezeAttempts;
        row.frozenDays = totalFrozenDays;
        row.permittedFreezeAttempts = permittedValues.attempts;
        row.permittedFreezeDays = permittedValues.days;
        row.status = status;
        row.freezeStartDate = firstFreezeDate ? formatISTDate(firstFreezeDate.toISOString()) : '';
        row.freezeEndDate = lastUnfreezeDate ? formatISTDate(lastUnfreezeDate.toISOString()) : '';
        
        // NEW: Add all freeze attempt pairs as a formatted string
        row.allFreezePairs = freezeAttemptPairs.length > 0 ? 
            freezeAttemptPairs.map((pair, index) => 
                `Attempt ${index + 1}: ${pair.startDate} to ${pair.endDate}`
            ).join(' | ') : '';
    }

    return memberRows;
}

// --- Member History Processor - HEAVILY OPTIMIZED ---
async function fetchAndProcessHistory(memberHostPairs) {
    console.log(`\nStep 4: Processing history for ${memberHostPairs.length} members with ${CONCURRENT_BATCHES} concurrent batches of ${BATCH_SIZE}...`);
    
    const uniqueMemberHostPairs = Array.from(
        new Set(memberHostPairs.map(pair => `${pair.memberId}:${pair.hostId}`))
    ).map(pair => {
        const [memberId, hostId] = pair.split(':');
        return { memberId: parseInt(memberId), hostId: hostId.toString() };
    });

    const allRows = [];
    const totalBatches = Math.ceil(uniqueMemberHostPairs.length / BATCH_SIZE);
    
    // Process batches concurrently
    for (let i = 0; i < uniqueMemberHostPairs.length; i += BATCH_SIZE * CONCURRENT_BATCHES) {
        const concurrentBatches = [];
        
        for (let j = 0; j < CONCURRENT_BATCHES && (i + j * BATCH_SIZE) < uniqueMemberHostPairs.length; j++) {
            const batchStart = i + j * BATCH_SIZE;
            const batchEnd = Math.min(batchStart + BATCH_SIZE, uniqueMemberHostPairs.length);
            const batch = uniqueMemberHostPairs.slice(batchStart, batchEnd);
            const batchIndex = Math.floor(batchStart / BATCH_SIZE) + 1;
            
            concurrentBatches.push(processBatchConcurrently(batch, batchIndex));
        }
        
        console.log(` -> Processing concurrent batches ${Math.floor(i / BATCH_SIZE) + 1}-${Math.min(Math.floor(i / BATCH_SIZE) + CONCURRENT_BATCHES, totalBatches)} of ${totalBatches}...`);
        
        const batchResults = await Promise.all(concurrentBatches);
        batchResults.forEach(results => allRows.push(...results));
        
        // Increased delay between batch groups to handle severe rate limiting
        if (i + BATCH_SIZE * CONCURRENT_BATCHES < uniqueMemberHostPairs.length) {
            await new Promise(resolve => setTimeout(resolve, 5000)); // Increased from 2000 to 5000ms
        }
    }

    return allRows;
}

// --- Cancellation Data Processor - OPTIMIZED ---
async function extractCancellationData(memberHostPairs) {
    console.log(`\nStep 4b: Extracting cancellation data for ${memberHostPairs.length} members...`);
    const allCancellations = [];

    const uniqueMemberHostPairs = Array.from(
        new Set(memberHostPairs.map(pair => `${pair.memberId}:${pair.hostId}`))
    ).map(pair => {
        const [memberId, hostId] = pair.split(':');
        return { memberId: parseInt(memberId), hostId: hostId.toString() };
    });

    // Use the same concurrent processing approach
    for (let i = 0; i < uniqueMemberHostPairs.length; i += BATCH_SIZE * CONCURRENT_BATCHES) {
        const concurrentBatches = [];
        
        for (let j = 0; j < CONCURRENT_BATCHES && (i + j * BATCH_SIZE) < uniqueMemberHostPairs.length; j++) {
            const batchStart = i + j * BATCH_SIZE;
            const batchEnd = Math.min(batchStart + BATCH_SIZE, uniqueMemberHostPairs.length);
            const batch = uniqueMemberHostPairs.slice(batchStart, batchEnd);
            
            concurrentBatches.push(processCancellationBatch(batch));
        }
        
        const batchResults = await Promise.all(concurrentBatches);
        batchResults.forEach(results => allCancellations.push(...results));
        
        if (i + BATCH_SIZE * CONCURRENT_BATCHES < uniqueMemberHostPairs.length) {
            await new Promise(resolve => setTimeout(resolve, 5000)); // Increased from 2000 to 5000ms for cancellations too
        }
    }
    
    console.log(`Found ${allCancellations.length} cancellation records.`);
    return allCancellations;
}

// NEW: Optimized cancellation batch processor
async function processCancellationBatch(batch) {
    const promises = batch.map(({ memberId, hostId }) =>
        retryApiCall(`https://api.momence.com/host/${hostId}/customers/${memberId}/history`, memberId, hostId)
        .catch(err => ({ error: err, memberId, hostId }))
    );

    const responses = await Promise.all(promises);
    const cancellations = [];
    
    for (let i = 0; i < responses.length; i++) {
        const response = responses[i];
        const { memberId: batchMemberId, hostId: batchHostId } = batch[i];
        
        // Handle undefined responses or responses with errors
        if (!response || response.error) {
            if (response && response.error) {
                console.warn(`Failed to get cancellation data for member ${response.memberId || batchMemberId} (host ${response.hostId || batchHostId}): ${response.error.message}`);
            } else {
                console.warn(`Failed to get cancellation data for member ${batchMemberId} (host ${batchHostId}): Undefined response`);
            }
            continue;
        }

        const historyEntries = response.data;
        if (!historyEntries || historyEntries.length === 0) continue;

        for (const entry of historyEntries) {
            if (entry.type === 'session' && entry.activities && entry.activities.length > 0) {
                for (const activity of entry.activities) {
                    if (activity.type === 'session-booking-cancelled-by-member' || 
                        activity.type === 'session-booking-cancelled-by-host') {
                        
                        cancellations.push({
                            memberId: formatValue(entry.memberId || batchMemberId), // Ensure memberId is populated
                            memberName: formatValue(entry.payingMemberName || entry.memberName),
                            hostId: formatValue(entry.hostId || batchHostId), // Ensure hostId is populated
                            sessionId: formatValue(entry.sessionId),
                            sessionName: formatValue(entry.sessionName),
                            sessionStartsAt: formatISTDate(entry.startsAt),
                            bookingId: formatValue(entry.bookingId),
                            cancellationType: formatValue(activity.type),
                            cancelledAt: formatISTDate(activity.createdAt),
                            cancelledByUserId: formatValue(activity.createdBy),
                            cancelledByUserName: formatValue(activity.triggeredBy ? 
                                `${activity.triggeredBy.firstName} ${activity.triggeredBy.lastName}` : ''),
                            locationId: formatValue(entry.locationId),
                            locationName: formatValue(entry.locationName),
                            teacherId: formatValue(entry.teacherId),
                            teacherName: formatValue(entry.teacherName),
                            isLateCancelled: formatValue(entry.isLateCancelled),
                            isCancelledAfterCutOff: formatValue(entry.isCancelledAfterCutOff),
                            membershipId: formatValue(entry.membershipId),
                            membershipName: formatValue(entry.membershipName),
                            boughtMembershipId: formatValue(entry.boughtMembershipId),
                            paymentMethod: formatValue(entry.paymentMethod),
                            paymentSource: formatValue(entry.paymentSource),
                            refundAmountInMoneyCredits: formatValue(activity.payload?.refundAmountInMoneyCredits || 0),
                            refundAmountInEventCredits: formatValue(activity.payload?.refundAmountInEventCredits || 0),
                            isMemberRefunded: formatValue(activity.payload?.isMemberRefunded || false)
                        });
                    }
                }
            }
        }
    }
    
    return cancellations;
}

// --- Google Sheets Writing - OPTIMIZED ---
async function writeToGoogleSheets(data) {
    if (!data || data.length === 0) {
        console.log("No data to write to Google Sheets.");
        return;
    }

    console.log(`\nStep 7: Writing ${data.length} rows to Google Sheets Freezes sheet...`);
    
    try {
        const auth = await getGoogleSheetsAuth();
        const sheets = google.sheets({ version: 'v4', auth });
        
        const headers = [
            'Member Name', 'Membership Name', 'Membership Id', 'Member Id', 'Bought Membership Id',
            'Host Id', 'Start Date', 'End Date', 'Classes Left', 'Usage Limit For Sessions',
            'Created At', 'Created By User Id', 'Created By User Name', 'Is Freezed', 'Is Voided',
            'Money Left', 'Payment Transaction Id', 'Sale Item Id', 'Membership Type',
            'Payment Method', 'Payment Source', 'Amount Paid', 'Sessions Attended', 'Location Id', 'Location Name',
            'Freeze Attempts', 'Frozen Days', 'Permitted Freeze Attempts', 'Permitted Freeze Days', 'Status',
            'Freeze Start Date', 'Freeze End Date', 'All Freeze Attempt Pairs'
        ];
        
        // NEW: Use batch writing for better performance with large datasets
        const SHEETS_BATCH_SIZE = 1000;
        
        // Clear the existing data first
        await sheets.spreadsheets.values.clear({
            spreadsheetId: SPREADSHEET_ID,
            range: `${SHEET_NAME}!A:AG`,
        });
        
        // Write headers first
        await sheets.spreadsheets.values.update({
            spreadsheetId: SPREADSHEET_ID,
            range: `${SHEET_NAME}!A1:AG1`,
            valueInputOption: 'RAW',
            resource: { values: [headers] }
        });
        
        // Write data in batches for better performance
        for (let i = 0; i < data.length; i += SHEETS_BATCH_SIZE) {
            const batchData = data.slice(i, i + SHEETS_BATCH_SIZE);
            const rows = batchData.map(item => [
                item.memberName || '',
                item.membershipName || '',
                item.membershipId || '',
                item.memberId || '',
                item.boughtMembershipId || '',
                item.hostId || '',
                item.startDate || '',
                item.endDate || '',
                item.classesLeft || '',
                item.usageLimitForSessions || '',
                item.createdAt || '',
                item.createdByUserId || '',
                item.createdByUserName || '',
                item.isFreezed || '',
                item.isVoided || '',
                item.moneyLeft || '',
                item.paymentTransactionId || '',
                item.saleItemId || '',
                item.membershipType || '',
                item.paymentMethod || '',
                item.paymentSource || '',
                item.amountPaid || '',
                item.sessionsAttended || '',
                item.locationId || '',
                item.locationName || '',
                item.freezeAttempts || '',
                item.frozenDays || '',
                item.permittedFreezeAttempts || '',
                item.permittedFreezeDays || '',
                item.status || '',
                item.freezeStartDate || '',
                item.freezeEndDate || '',
                item.allFreezePairs || ''
            ]);
            
            const startRow = i + 2; // +2 because row 1 has headers and sheets are 1-indexed
            await sheets.spreadsheets.values.update({
                spreadsheetId: SPREADSHEET_ID,
                range: `${SHEET_NAME}!A${startRow}:AG${startRow + rows.length - 1}`,
                valueInputOption: 'RAW',
                resource: { values: rows }
            });
            
            console.log(`   -> Written batch ${Math.floor(i / SHEETS_BATCH_SIZE) + 1} (${Math.min(i + SHEETS_BATCH_SIZE, data.length)} of ${data.length})`);
        }
        
        console.log(`✅ Successfully wrote ${data.length} rows to Google Sheets Freezes sheet`);
    } catch (error) {
        console.error('Error writing to Google Sheets Freezes sheet:', error.message);
        throw error;
    }
}

async function writeCancellationsToGoogleSheets(cancellationData) {
    if (!cancellationData || cancellationData.length === 0) {
        console.log("No cancellation data to write to Google Sheets.");
        return;
    }

    console.log(`\nStep 8: Writing ${cancellationData.length} rows to Google Sheets Cancellations sheet...`);
    
    try {
        const auth = await getGoogleSheetsAuth();
        const sheets = google.sheets({ version: 'v4', auth });
        
        const headers = [
            'Member Id', 'Member Name', 'Host Id', 'Session Id', 'Session Name', 'Session Starts At',
            'Booking Id', 'Cancellation Type', 'Cancelled At', 'Cancelled By User Id', 'Cancelled By User Name',
            'Location Id', 'Location Name', 'Teacher Id', 'Teacher Name', 'Is Late Cancelled', 
            'Is Cancelled After Cut Off', 'Membership Id', 'Membership Name', 'Bought Membership Id',
            'Payment Method', 'Payment Source', 'Refund Amount Money Credits', 'Refund Amount Event Credits', 'Is Member Refunded'
        ];
        
        const SHEETS_BATCH_SIZE = 1000;
        
        // Clear the existing data first
        await sheets.spreadsheets.values.clear({
            spreadsheetId: SPREADSHEET_ID,
            range: `Cancellations!A:Y`,
        });
        
        // Write headers first
        await sheets.spreadsheets.values.update({
            spreadsheetId: SPREADSHEET_ID,
            range: `Cancellations!A1:Y1`,
            valueInputOption: 'RAW',
            resource: { values: [headers] }
        });
        
        // Write cancellation data in batches
        for (let i = 0; i < cancellationData.length; i += SHEETS_BATCH_SIZE) {
            const batchData = cancellationData.slice(i, i + SHEETS_BATCH_SIZE);
            const rows = batchData.map(item => [
                item.memberId || '',
                item.memberName || '',
                item.hostId || '',
                item.sessionId || '',
                item.sessionName || '',
                item.sessionStartsAt || '',
                item.bookingId || '',
                item.cancellationType || '',
                item.cancelledAt || '',
                item.cancelledByUserId || '',
                item.cancelledByUserName || '',
                item.locationId || '',
                item.locationName || '',
                item.teacherId || '',
                item.teacherName || '',
                item.isLateCancelled || '',
                item.isCancelledAfterCutOff || '',
                item.membershipId || '',
                item.membershipName || '',
                item.boughtMembershipId || '',
                item.paymentMethod || '',
                item.paymentSource || '',
                item.refundAmountInMoneyCredits || '',
                item.refundAmountInEventCredits || '',
                item.isMemberRefunded || ''
            ]);
            
            const startRow = i + 2;
            await sheets.spreadsheets.values.update({
                spreadsheetId: SPREADSHEET_ID,
                range: `Cancellations!A${startRow}:Y${startRow + rows.length - 1}`,
                valueInputOption: 'RAW',
                resource: { values: rows }
            });
            
            console.log(`   -> Written cancellation batch ${Math.floor(i / SHEETS_BATCH_SIZE) + 1} (${Math.min(i + SHEETS_BATCH_SIZE, cancellationData.length)} of ${cancellationData.length})`);
        }
        
        console.log(`✅ Successfully wrote ${cancellationData.length} rows to Google Sheets Cancellations sheet`);
    } catch (error) {
        console.error('Error writing to Google Sheets Cancellations sheet:', error.message);
        throw error;
    }
}

// --- File Writing Functions - OPTIMIZED ---
async function writeToJson(data) {
    if (data.length === 0) {
        console.log("No data to write to JSON.");
        return;
    }
    console.log(`\nStep 5: Writing ${data.length} rows to ${OUTPUT_JSON_PATH}...`);
    try {
        // NEW: Use streaming for large datasets
        const fs = require('fs');
        const writeStream = fs.createWriteStream(OUTPUT_JSON_PATH);
        
        writeStream.write('[\n');
        for (let i = 0; i < data.length; i++) {
            writeStream.write(JSON.stringify(data[i]));
            if (i < data.length - 1) {
                writeStream.write(',\n');
            } else {
                writeStream.write('\n');
            }
        }
        writeStream.write(']');
        writeStream.end();
        
        console.log("✅ Successfully wrote data to data.json");
    } catch (error) {
        console.error("Error writing to JSON file:", error);
    }
}

async function writeToCsv(data) {
    if (data.length === 0) {
        console.log("No data to write to CSV.");
        return;
    }
    console.log(`\nStep 6: Writing ${data.length} rows to ${OUTPUT_CSV_PATH}...`);

    const csvWriter = createObjectCsvWriter({
        path: OUTPUT_CSV_PATH,
        header: [
            { id: 'timestamp', title: 'Timestamp' },
            { id: 'historyType', title: 'History Type' },
            { id: 'historyId', title: 'History Id' },
            { id: 'discountCode', title: 'Discount Code' },
            { id: 'memberName', title: 'Member Name' },
            { id: 'membershipName', title: 'Membership Name' },
            { id: 'membershipId', title: 'Membership Id' },
            { id: 'memberId', title: 'Member Id' },
            { id: 'boughtMembershipId', title: 'Bought Membership Id' },
            { id: 'hostId', title: 'Host Id' },
            { id: 'startDate', title: 'Start Date' },
            { id: 'endDate', title: 'End Date' },
            { id: 'classesLeft', title: 'Classes Left' },
            { id: 'usageLimitForSessions', title: 'Usage Limit For Sessions' },
            { id: 'createdAt', title: 'Created At' },
            { id: 'paymentMethod', title: 'Payment Method' },
            { id: 'paymentSource', title: 'Payment Source' },
            { id: 'amountPaid', title: 'Amount Paid' },
            { id: 'sessionsAttended', title: 'Sessions Attended' },
            { id: 'freezeAttempts', title: 'Freeze Attempts' },
            { id: 'frozenDays', title: 'Frozen Days' },
            { id: 'permittedFreezeAttempts', title: 'Permitted Freeze Attempts' },
            { id: 'permittedFreezeDays', title: 'Permitted Freeze Days' },
            { id: 'status', title: 'Status' },
            { id: 'allFreezePairs', title: 'All Freeze Attempt Pairs' }
        ]
    });

    try {
        // NEW: Write in chunks for better memory management with large datasets
        const CHUNK_SIZE = 1000;
        for (let i = 0; i < data.length; i += CHUNK_SIZE) {
            const chunk = data.slice(i, i + CHUNK_SIZE);
            if (i === 0) {
                await csvWriter.writeRecords(chunk);
            } else {
                // Append subsequent chunks
                const csvWriterAppend = createObjectCsvWriter({
                    path: OUTPUT_CSV_PATH,
                    header: [
                        { id: 'timestamp', title: 'Timestamp' },
                        { id: 'historyType', title: 'History Type' },
                        { id: 'historyId', title: 'History Id' },
                        { id: 'discountCode', title: 'Discount Code' },
                        { id: 'memberName', title: 'Member Name' },
                        { id: 'membershipName', title: 'Membership Name' },
                        { id: 'membershipId', title: 'Membership Id' },
                        { id: 'memberId', title: 'Member Id' },
                        { id: 'boughtMembershipId', title: 'Bought Membership Id' },
                        { id: 'hostId', title: 'Host Id' },
                        { id: 'startDate', title: 'Start Date' },
                        { id: 'endDate', title: 'End Date' },
                        { id: 'classesLeft', title: 'Classes Left' },
                        { id: 'usageLimitForSessions', title: 'Usage Limit For Sessions' },
                        { id: 'createdAt', title: 'Created At' },
                        { id: 'paymentMethod', title: 'Payment Method' },
                        { id: 'paymentSource', title: 'Payment Source' },
                        { id: 'amountPaid', title: 'Amount Paid' },
                        { id: 'sessionsAttended', title: 'Sessions Attended' },
                        { id: 'freezeAttempts', title: 'Freeze Attempts' },
                        { id: 'frozenDays', title: 'Frozen Days' },
                        { id: 'permittedFreezeAttempts', title: 'Permitted Freeze Attempts' },
                        { id: 'permittedFreezeDays', title: 'Permitted Freeze Days' },
                        { id: 'status', title: 'Status' },
                        { id: 'allFreezePairs', title: 'All Freeze Attempt Pairs' }
                    ],
                    append: true
                });
                await csvWriterAppend.writeRecords(chunk);
            }
            console.log(`   -> Written CSV chunk ${Math.floor(i / CHUNK_SIZE) + 1} (${Math.min(i + CHUNK_SIZE, data.length)} of ${data.length})`);
        }
        console.log("✅ Successfully wrote data to freezes.csv");
    } catch (error) {
        console.error("Error writing to CSV file:", error);
    }
}

// --- Main Function - OPTIMIZED ---
async function main() {
    const startTime = Date.now();
    console.log(`🚀 Starting optimized freeze history processing at ${new Date().toISOString()}`);
    
    try {
        // Step 1: Fetch data from Google Sheets (or fallback to async reports)
        const { memberHostPairs } = await fetchDataFromGoogleSheets();

        if (memberHostPairs && memberHostPairs.length > 0) {
            console.log(`📊 Processing ${memberHostPairs.length} unique member/host pairs`);
            
            // Step 2: Process member history data with optimized concurrent batching
            const processedData = await fetchAndProcessHistory(memberHostPairs);
            
            // Step 3: Process cancellation data concurrently with history data
            const cancellationPromise = extractCancellationData(memberHostPairs);
            
            // Step 4: Write processed data to files
            const writePromises = [
                writeToJson(processedData),
                writeToCsv(processedData),
                writeToGoogleSheets(processedData)
            ];
            
            // Step 5: Wait for both cancellation processing and file writing
            const [cancellationData] = await Promise.all([cancellationPromise, ...writePromises]);
            
            // Step 6: Write cancellation data
            await writeCancellationsToGoogleSheets(cancellationData);
            
            const endTime = Date.now();
            const totalTime = (endTime - startTime) / 1000;
            
            console.log(`\n🎉 PROCESSING COMPLETE!`);
            console.log(`   • Processed: ${processedData.length} membership records`);
            console.log(`   • Cancellations: ${cancellationData.length} records`);
            console.log(`   • Total time: ${totalTime.toFixed(2)} seconds`);
            console.log(`   • Average: ${(processedData.length / totalTime).toFixed(2)} records/second`);
            
        } else {
            console.log("❌ No member data found from Google Sheets to process.");
        }
    } catch (error) {
        console.error("\n💥 An unexpected error occurred during the script execution:", error);
        process.exit(1);
    }
}

// NEW: Process monitoring and cleanup
process.on('SIGINT', () => {
    console.log('\n⚠️ Process interrupted by user');
    process.exit(0);
});

process.on('uncaughtException', (error) => {
    console.error('💥 Uncaught Exception:', error);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
});

// Run the script
main();