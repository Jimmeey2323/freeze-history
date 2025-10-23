export default async function handler(req, res) {
    // CORS headers
    const corsHeaders = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (req.method === 'OPTIONS') {
        return res.status(200).json({});
    }

    if (req.method !== 'POST') {
        return res.status(405).json({ 
            ...corsHeaders,
            error: 'Method not allowed. Use POST to refresh data.' 
        });
    }

    try {
        // Clear the cache to force fresh data fetch
        global.cachedData = null;
        global.lastFetch = null;

        return res.status(200).json({
            ...corsHeaders,
            message: 'Cache cleared. Next data request will fetch fresh data.',
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        console.error('Error clearing cache:', error);
        return res.status(500).json({
            ...corsHeaders,
            error: 'Failed to clear cache',
            details: error.message
        });
    }
}