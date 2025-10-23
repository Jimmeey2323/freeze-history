import axios from 'axios';

// CORS headers for browser requests
const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
};

export default async function handler(req, res) {
    // Handle preflight requests
    if (req.method === 'OPTIONS') {
        return res.status(200).json({});
    }

    try {
        console.log('Starting data fetch process...');
        
        // Environment variables validation
        const requiredEnvVars = [
            'MOMENCE_ALL_COOKIES',
            'GOOGLE_CLIENT_ID', 
            'GOOGLE_CLIENT_SECRET',
            'GOOGLE_REFRESH_TOKEN',
            'SPREADSHEET_ID'
        ];
        
        for (const envVar of requiredEnvVars) {
            if (!process.env[envVar]) {
                console.error(`Missing environment variable: ${envVar}`);
                return res.status(500).json({ 
                    error: `Server configuration error: Missing ${envVar}`,
                    details: 'Please contact the administrator'
                });
            }
        }

        // Google OAuth Configuration
        const GOOGLE_OAUTH = {
            CLIENT_ID: process.env.GOOGLE_CLIENT_ID,
            CLIENT_SECRET: process.env.GOOGLE_CLIENT_SECRET,
            REFRESH_TOKEN: process.env.GOOGLE_REFRESH_TOKEN,
            TOKEN_URL: "https://oauth2.googleapis.com/token"
        };

        const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
        const MOMENCE_ALL_COOKIES = process.env.MOMENCE_ALL_COOKIES;

        // Check if we should fetch fresh data or return cached data
        const shouldFetchFresh = req.query.refresh === 'true' || !global.cachedData || 
                                (global.lastFetch && Date.now() - global.lastFetch > 300000); // 5 minutes cache

        if (!shouldFetchFresh && global.cachedData) {
            console.log('Returning cached data');
            return res.status(200).json({
                ...corsHeaders,
                'Content-Type': 'application/json',
                data: global.cachedData,
                cached: true,
                lastFetch: global.lastFetch
            });
        }

        console.log('Fetching fresh data...');

        // Get Google Access Token
        const tokenResponse = await axios.post(GOOGLE_OAUTH.TOKEN_URL, {
            client_id: GOOGLE_OAUTH.CLIENT_ID,
            client_secret: GOOGLE_OAUTH.CLIENT_SECRET,
            refresh_token: GOOGLE_OAUTH.REFRESH_TOKEN,
            grant_type: 'refresh_token'
        });

        const accessToken = tokenResponse.data.access_token;

        // Read data from Google Sheets
        const sheetsUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/Freezes!A:AZ?majorDimension=ROWS`;
        const sheetsResponse = await axios.get(sheetsUrl, {
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            }
        });

        const rows = sheetsResponse.data.values;
        if (!rows || rows.length === 0) {
            return res.status(404).json({
                ...corsHeaders,
                error: 'No data found in spreadsheet'
            });
        }

        // Convert to JSON format
        const headers = rows[0];
        const data = rows.slice(1).map(row => {
            const obj = {};
            headers.forEach((header, index) => {
                obj[header] = row[index] || '';
            });
            return obj;
        });

        // Cache the data
        global.cachedData = data;
        global.lastFetch = Date.now();

        console.log(`Successfully fetched ${data.length} records`);

        return res.status(200).json({
            ...corsHeaders,
            'Content-Type': 'application/json',
            data: data,
            cached: false,
            lastFetch: global.lastFetch,
            recordCount: data.length
        });

    } catch (error) {
        console.error('Error in fetch-data function:', error);
        
        return res.status(500).json({
            ...corsHeaders,
            error: 'Failed to fetch data',
            details: error.message,
            timestamp: new Date().toISOString()
        });
    }
}