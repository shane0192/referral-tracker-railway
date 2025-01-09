# Paperboy Referral Tracker

A web application for tracking and managing referral partnerships between creators, with features for monitoring referral metrics, managing accounts, and analyzing partnership trends.

## Architecture Overview

The system consists of two main components:
1. Local Scraper: Runs on your machine using launchd, collecting data from ConvertKit
2. Web Interface: Hosted on Heroku, providing data visualization and analysis

Both components connect to the same Railway PostgreSQL database, ensuring data consistency.

## Project Structure

```
referral_tracker/
├── src/
│   ├── api/
│   │   ├── routes.py          # Main API endpoints and server logic
│   │   └── __init__.py
│   ├── scraper/
│   │   ├── convertkit_scraper.py  # ConvertKit data scraping logic
│   │   └── scheduler.py       # Automated scraping scheduler
│   ├── data/
│   │   ├── db_manager.py      # Database management and operations
│   │   └── referral_data.csv  # Backup CSV data
│   └── clock.py              # Scheduler process
├── config/                   # Configuration files
│   ├── .env                 # Environment variables
│   ├── requirements.txt     # Python dependencies
│   └── runtime.txt         # Python runtime version
├── logs/                    # Log files directory
│   ├── scheduler_output.log # Scheduler standard output
│   └── scheduler_error.log  # Scheduler error output
├── data/
│   ├── referral_viewer.html # Main frontend interface
│   ├── referral_data.db    # SQLite database (development)
│   └── avatars/            # Partner avatar images
├── automation_chrome_profile/ # Chrome profile for automation
├── chrome-for-testing/      # ChromeDriver files
├── scripts/
│   └── run_scheduler.sh    # Scheduler startup script
└── com.paperboy.referral-tracker.plist  # Launch agent configuration
```

## Core Functionality

### 1. Account Management
- Enable/disable tracking for specific accounts
- View account status (available, has data, enabled)
- Automatic inclusion of "Demo Client" for testing
- Account configuration stored in `config/enabled_accounts.json`

### 2. Data Collection
- Automated scraping of ConvertKit referral data
- Scheduled data collection via `scheduler.py`
- Support for multiple accounts
- Error handling and retry logic
- Data storage in SQLite database

### ConvertKit Scraper (`convertkit_scraper.py`)
The ConvertKit scraper is a robust automation tool that handles data collection from the ConvertKit Creator Network. Key features include:

#### Authentication & Session Management:
- Persistent Chrome profile for session management
- Automatic handling of login and 2FA verification
- Session recovery and retry mechanisms
- Headless mode support for background operation

#### Multi-Account Support:
- Dynamic account switching
- Account validation and verification
- Parallel scraping capabilities
- Account state tracking

#### Data Collection Features:
- Scrapes both incoming ("Recommending me") and outgoing ("My Recommendations") referrals
- Captures subscriber counts and conversion rates
- Profile avatar collection and storage
- Data validation and integrity checks

#### Error Handling & Reliability:
- Automatic session recovery
- Multiple retry attempts with exponential backoff
- Detailed error logging and screenshots
- Data validation before storage

#### Performance & Optimization:
- Chrome profile reuse for faster operation
- Efficient DOM traversal
- Parallel processing capabilities
- Memory-efficient data handling

### 3. Frontend Interface (`referral_viewer.html`)
- Dashboard view with referral metrics
- Account selection dropdown
- Date range filtering
- Partnership trend visualization
- Data tables for detailed metrics

### 4. API Endpoints (`routes.py`)

#### Main Endpoints:
- `/api/available-accounts`: Get list of available and enabled accounts
- `/api/partnership-metrics`: Get referral metrics for accounts
- `/api/trends/<account_name>`: Get trend data for specific account
- `/api/partnership-trends`: Get detailed partnership trend data
- `/api/partnership-recommendations`: Get partnership recommendations

#### Admin Endpoints:
- `/admin/database`: Database management interface
- `/admin/cleanup-duplicates`: Remove duplicate entries
- `/admin/cleanup-initial-data`: Clean partial data
- `/admin/bulk-delete`: Delete multiple records

### 5. Database Schema

#### ReferralData Table:
- `id`: Primary key
- `date`: Timestamp
- `account_name`: Account identifier
- `recommending_me`: JSON array of incoming referrals
- `my_recommendations`: JSON array of outgoing referrals

### 6. Key Features

#### Partnership Metrics:
- Track incoming and outgoing referrals
- Calculate referral imbalances
- Monitor growth trends
- Generate partnership recommendations

#### Data Visualization:
- Trend charts
- Partnership balance metrics
- Growth indicators
- Historical data comparison

#### Account Management:
- Enable/disable accounts
- Track account status
- Manage data collection

#### Data Maintenance:
- Duplicate detection and cleanup
- Partial data cleanup
- Bulk record management
- CSV data import/export

### 7. Demo Mode
- Built-in demo client
- Generated sample data
- Test environment for features
- No live data requirements

## Setup and Configuration

### Requirements:
- Python 3.7+
- SQLite3
- Chrome/Chromium (for scraping)
- Required Python packages in `requirements.txt`

### Environment Setup:
1. Install dependencies: `pip install -r requirements.txt`
2. Configure enabled accounts in `config/enabled_accounts.json`
3. Set up scheduler (optional): `scripts/run_scheduler.sh`

### Database:
- Located at `data/referral_data.db`
- Automatic creation on first run
- Backup CSV data in `src/data/referral_data.csv`

## Common Operations

### Adding New Accounts:
1. Access account manager in UI
2. Select new accounts to enable
3. Save changes
4. Verify in enabled_accounts.json

### Data Cleanup:
1. Use admin interface at `/admin/database`
2. Select cleanup operation:
   - Remove duplicates
   - Clean partial data
   - Bulk delete records

### Monitoring:
- Check last scrape time in UI
- View error logs
- Monitor database growth
- Track account status

## Troubleshooting

### Common Issues:
1. Scraper failures:
   - Check Chrome/Chromium installation
   - Verify network connectivity
   - Check ConvertKit login status

2. Missing data:
   - Verify account is enabled
   - Check last successful scrape
   - Look for partial data flags

3. UI issues:
   - Clear browser cache
   - Check console for errors
   - Verify API connectivity

## Development Notes

### Adding Features:
1. Frontend changes in `referral_viewer.html`
2. API endpoints in `routes.py`
3. Database changes in `db_manager.py`
4. Update README as needed

### Testing:
- Use Demo Client for feature testing
- Verify with small data sets first
- Check all account scenarios
- Test error handling

### Security:
- API requires authentication
- Account validation
- Data access controls
- Error message sanitization 