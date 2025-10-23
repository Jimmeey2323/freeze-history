# Freeze History Dashboard

Enhanced membership dashboard with freeze history tracking, drill-down functionality, and multi-view options.

## Features

- 📊 **Enhanced Datatable**: 24+ columns with comprehensive member information
- 🔍 **Drill-Down Functionality**: Expandable rows showing detailed freeze attempt history
- 🎛️ **Multi-View Options**: Summary, Detailed, and Freeze-Focused views
- 🎨 **Dark Theme**: Professional responsive design
- ⚡ **Real-time Data**: Serverless API integration with Google Sheets
- 🔄 **Auto-refresh**: Cached data with smart refresh logic

## Deployment on Vercel

### Prerequisites

1. **Google Sheets API Setup**:
   - Create a Google Cloud Project
   - Enable Google Sheets API
   - Create OAuth 2.0 credentials
   - Get your spreadsheet ID

2. **Environment Variables**:
   - `MOMENCE_ALL_COOKIES`: Your Momence session cookies
   - `GOOGLE_CLIENT_ID`: OAuth client ID
   - `GOOGLE_CLIENT_SECRET`: OAuth client secret  
   - `GOOGLE_REFRESH_TOKEN`: OAuth refresh token
   - `SPREADSHEET_ID`: Your Google Sheets ID

### Step 1: Install Vercel CLI

```bash
npm install -g vercel
```

### Step 2: Login to Vercel

```bash
vercel login
```

### Step 3: Deploy

From your project directory:

```bash
vercel
```

Follow the prompts:
- Set up and deploy? **Y**
- Which scope? Select your account
- Link to existing project? **N** (for first deployment)
- Project name: `freeze-history` (or your preferred name)
- Directory: `./` (current directory)

### Step 4: Set Environment Variables

After deployment, set your environment variables:

```bash
vercel env add MOMENCE_ALL_COOKIES
vercel env add GOOGLE_CLIENT_ID
vercel env add GOOGLE_CLIENT_SECRET
vercel env add GOOGLE_REFRESH_TOKEN
vercel env add SPREADSHEET_ID
```

Or set them in the Vercel dashboard:
1. Go to your project in Vercel dashboard
2. Navigate to Settings → Environment Variables
3. Add all required variables

### Step 5: Redeploy

After setting environment variables:

```bash
vercel --prod
```

## API Endpoints

- `GET /api/fetch-data` - Fetch membership data (with 5-minute caching)
- `POST /api/refresh` - Clear cache and force fresh data fetch

## Local Development

1. Install dependencies:
```bash
npm install
```

2. Create `.env` file with required variables

3. Start development server:
```bash
vercel dev
```

## Project Structure

```
├── api/
│   ├── fetch-data.js      # Main data fetching serverless function
│   └── refresh.js         # Cache refresh endpoint
├── index.html             # Main dashboard interface
├── app.js                 # Frontend JavaScript
├── style.css              # Styling and themes
├── vercel.json            # Vercel configuration
└── package.json           # Dependencies and scripts
```

## Data Flow

1. **Frontend** requests data from `/api/fetch-data`
2. **Serverless function** checks cache (5-minute TTL)
3. If cache miss, fetches from **Google Sheets** using OAuth
4. Data is cached and returned to frontend
5. **Dashboard** renders with drill-down functionality

## Cache Management

- Data is cached for 5 minutes to improve performance
- Use `?refresh=true` parameter to force fresh data
- POST to `/api/refresh` to clear cache programmatically

## Troubleshooting

### Common Issues

1. **"Could not load data"**: Check environment variables are set correctly
2. **OAuth errors**: Verify Google OAuth credentials and refresh token
3. **Spreadsheet access**: Ensure the service account has access to your sheet

### Debug Mode

Add `?debug=true` to your URL to see detailed error messages.

## Security Notes

- All sensitive credentials are stored as Vercel environment variables
- OAuth tokens are refreshed automatically
- CORS is properly configured for browser access
- Rate limiting is implemented to prevent API abuse

## Performance

- **Caching**: 5-minute server-side cache reduces API calls
- **Lazy Loading**: Large datasets are paginated
- **Serverless**: Auto-scaling based on demand
- **CDN**: Static assets served via Vercel's global CDN