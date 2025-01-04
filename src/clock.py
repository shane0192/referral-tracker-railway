from apscheduler.schedulers.blocking import BlockingScheduler
from scraper.scheduler import ScraperScheduler
import os
import json
from datetime import datetime, timedelta
import logging
import sys

# Set up logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler()
    ]
)

# Add debug file handler separately
debug_handler = logging.FileHandler("scheduler.debug.log")
debug_handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(debug_handler)

logger = logging.getLogger(__name__)

scheduler = BlockingScheduler()
scraper_scheduler = ScraperScheduler()

# Add debug logging for startup
def log_startup_info():
    """Log detailed startup information"""
    logger.info("=" * 50)
    logger.info("Scheduler starting up")
    logger.info(f"Current time: {datetime.now()}")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    try:
        with open('last_run.json', 'r') as f:
            last_run_data = json.load(f)
            logger.info(f"Last run data: {last_run_data}")
    except Exception as e:
        logger.info(f"No last run data found: {str(e)}")
    logger.info("=" * 50)

def check_last_successful_run():
    """Check if we had a successful run for ALL accounts today"""
    try:
        with open('last_run.json', 'r') as f:
            data = json.load(f)
            last_run = datetime.fromisoformat(data.get('last_run'))
            pending_accounts = set(data.get('pending_accounts', []))
            
            # Only consider it successful if there are no pending accounts
            if not pending_accounts and last_run.date() == datetime.now().date():
                return True
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    return False

@scheduler.scheduled_job('cron', hour=6, minute=0)
def daily_scrape():
    """Main daily scrape at 6am"""
    logger.info(f"Running scheduled scrape at {datetime.now()}")
    success = scraper_scheduler.run_scraper()
    
    if not success:
        logger.info("Daily scrape unsuccessful - hourly retries will be attempted")

@scheduler.scheduled_job('cron', hour='7-23')
def hourly_retry():
    """Hourly retry between 7am and 11pm"""
    if not check_last_successful_run():
        logger.info(f"Running retry scrape at {datetime.now()}")
        scraper_scheduler.run_scraper()

if __name__ == '__main__':
    log_startup_info()
    logger.info("Starting scheduler...")
    scheduler.start() 