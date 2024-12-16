from apscheduler.schedulers.blocking import BlockingScheduler
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.scraper.convertkit_scraper import ConvertKitScraper, ALLOWED_ACCOUNTS
from datetime import datetime, timedelta
import logging
import json
import requests
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ScraperScheduler:
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.slack_webhook = "https://hooks.slack.com/services/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"
        self.last_run_file = 'last_run.json'
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        self.pending_accounts = set()
        self.successful_accounts = set()
        
    def send_notification(self, message):
        """Send Slack notification"""
        try:
            payload = {"text": message}
            response = requests.post(self.slack_webhook, json=payload)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")

    def get_last_run(self):
        """Get the last successful run time and account states"""
        try:
            if os.path.exists(self.last_run_file):
                with open(self.last_run_file, 'r') as f:
                    data = json.load(f)
                    if data:
                        self.pending_accounts = set(data.get('pending_accounts', []))
                        self.successful_accounts = set(data.get('successful_accounts', []))
                        if 'last_run' in data:
                            return datetime.fromisoformat(data['last_run'])
            return None
        except Exception as e:
            logger.error(f"Error reading last run time: {str(e)}")
            return None
        
    def save_last_run(self):
        """Save the last successful run time and account states"""
        try:
            with open(self.last_run_file, 'w') as f:
                json.dump({
                    'last_run': datetime.now().isoformat(),
                    'pending_accounts': list(self.pending_accounts),
                    'successful_accounts': list(self.successful_accounts)
                }, f)
        except Exception as e:
            logger.error(f"Error saving last run time: {str(e)}")

    def should_run(self, force=False):
        """Check if scraper should run"""
        if force:
            logger.info("Force run requested - ignoring last run time")
            return True
        
        try:
            last_run = self.get_last_run()
            if last_run:
                next_run = last_run.replace(hour=6, minute=0, second=0, microsecond=0)
                if last_run.hour >= 6:
                    next_run += timedelta(days=1)
                
                logger.info(f"Last successful run: {last_run}")
                logger.info(f"Next scheduled run: {next_run}")
                
                if datetime.now() < next_run:
                    logger.info("Skipping run - too soon since last successful run")
                    return False
                
            return True
        except Exception as e:
            logger.error(f"Error checking run schedule: {str(e)}")
            return True  # Run on error to be safe

    def run_scraper(self, force=False):
        """Run the scraper"""
        try:
            current_time = datetime.now()
            logger.info(f"Starting scraper run at {current_time}")
            
            # Clear successful accounts at start of day
            if current_time.hour == 6 and current_time.minute < 5:
                logger.info("Starting new day - clearing successful accounts")
                self.successful_accounts.clear()
            
            # Determine which accounts to process
            is_hourly = not force and self.pending_accounts
            if is_hourly:
                logger.info(f"Hourly run - processing pending accounts: {self.pending_accounts}")
                target_accounts = self.pending_accounts.copy()
            else:
                logger.info("Full run - processing all accounts")
                target_accounts = set(ALLOWED_ACCOUNTS)
            
            if not target_accounts:
                logger.info("No accounts to process")
                return
            
            scraper = None
            retry_count = 0
            validation_messages = []
            
            while retry_count < self.max_retries:
                try:
                    if scraper:
                        try:
                            scraper.driver.quit()
                        except:
                            pass
                    
                    # Initialize scraper without headless argument
                    scraper = ConvertKitScraper()  # Remove headless=True
                    
                    # If login fails, restart with visible browser
                    if not scraper.login():
                        logger.info("Login failed - restarting with visible browser for manual login")
                        scraper.driver.quit()
                        scraper = ConvertKitScraper()  # Initialize without headless mode
                        self.send_notification("🚨 ConvertKit login needed - please log in manually")
                        return

                    success = True
                    failed_accounts = []
                    
                    accounts = scraper.get_available_accounts()
                    for account in accounts:
                        # Skip if already successful today
                        if account['name'] in self.successful_accounts:
                            logger.info(f"Skipping {account['name']} - already successful today")
                            continue
                            
                        try:
                            scraper.switch_to_account(account)
                            
                            # First scrape the data
                            data = scraper.scrape_referral_data()
                            
                            # Then validate it
                            is_valid, message = scraper.validate_scrape_data(data, account['name'])
                            validation_messages.append(message)
                            
                            if not is_valid:
                                failed_accounts.append(account['name'])
                                success = False
                                
                        except Exception as e:
                            logger.error(f"Failed to scrape {account['name']}: {str(e)}")
                            failed_accounts.append(account['name'])
                            success = False
                            
                            # Try to recover the session
                            try:
                                if not scraper.restart_session():
                                    raise Exception("Failed to restart session")
                            except Exception as recovery_error:
                                logger.error(f"Session recovery failed: {str(recovery_error)}")
                                break

                    if success:
                        self.save_last_run()
                        self.send_notification("✅ ConvertKit data successfully collected for today")
                    else:
                        # Send detailed validation report
                        validation_report = "\n".join(validation_messages)
                        failed_list = ", ".join(failed_accounts)
                        self.send_notification(
                            f"⚠️ ConvertKit data collection issues:\n"
                            f"Failed accounts: {failed_list}\n\n"
                            f"Validation Report:\n{validation_report}"
                        )
                        
                        retry_count += 1
                        if retry_count < self.max_retries:
                            logger.info(f"Retrying entire scrape (attempt {retry_count + 1}/{self.max_retries})")
                            time.sleep(self.retry_delay)
                        continue

                    return  # Exit successfully if everything worked

                except Exception as e:
                    logger.error(f"Scraper error (attempt {retry_count + 1}/{self.max_retries}): {str(e)}", exc_info=True)
                    retry_count += 1
                    
                    if retry_count >= self.max_retries:
                        self.send_notification(f"❌ ConvertKit scraper error: {str(e)}")
                    else:
                        logger.info(f"Retrying after error (attempt {retry_count + 1}/{self.max_retries})")
                        time.sleep(self.retry_delay)
                        
                finally:
                    if scraper:
                        try:
                            scraper.driver.quit()
                        except:
                            pass  # Ignore cleanup errors

        except Exception as e:
            logger.error(f"Critical scheduler error: {str(e)}", exc_info=True)
            self.send_notification(f"🚨 Critical scheduler error: {str(e)}")
            raise

    def start(self):
        """Start the scheduler"""
        try:
            logger.info("=" * 50)
            logger.info("SCHEDULER STARTUP")
            logger.info(f"Current time: {datetime.now()}")
            
            # Print all scheduled jobs
            jobs = self.scheduler.get_jobs()
            logger.info("Scheduled jobs:")
            for job in jobs:
                logger.info(f"- {job.id}: Next run at {job.next_run_time}")
            
            # Print account states
            logger.info(f"Pending accounts: {self.pending_accounts}")
            logger.info(f"Successful accounts: {self.successful_accounts}")
            
            # Start with a clean state
            self.successful_accounts.clear()
            self.pending_accounts.clear()
            
            # Calculate next run times manually
            now = datetime.now()
            next_daily = now.replace(hour=6, minute=0, second=0, microsecond=0)
            if now.hour >= 6:
                next_daily += timedelta(days=1)
            
            next_hourly = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            
            # Add jobs with misfire grace time
            daily_job = self.scheduler.add_job(
                self.run_scraper,
                'cron',
                hour=6,
                minute=0,
                id='daily_scrape',
                misfire_grace_time=3600  # Allow job to run up to 1 hour late
            )
            
            hourly_job = self.scheduler.add_job(
                self.run_scraper,
                'interval',
                hours=1,
                id='hourly_check',
                misfire_grace_time=3600
            )
            
            # Log schedule info
            logger.info("Schedule configuration:")
            logger.info(f"Daily job next run: {next_daily}")
            logger.info(f"Hourly job next run: {next_hourly}")
            
            # Check if we need to run now
            last_run = self.get_last_run()
            logger.info(f"Last recorded run: {last_run}")
            
            if not last_run or (datetime.now() - last_run).days >= 1:
                logger.info("No recent run detected - running now...")
                self.run_scraper(force=True)
            
            # Start the scheduler with better error handling
            logger.info("Starting scheduler...")
            self.scheduler.start()
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {str(e)}", exc_info=True)
            self.send_notification("🚨 Scheduler failed to start")
            raise

    def get_next_run_time(self):
        """Get the next scheduled run time"""
        try:
            jobs = self.scheduler.get_jobs()
            if jobs:
                # Use _get_run_times() instead of next_run_time
                next_runs = jobs[0]._get_run_times(datetime.now())
                if next_runs:
                    next_run = next_runs[0]
                    logger.info(f"Next scheduled run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                    return next_run
            return None
        except Exception as e:
            logger.error(f"Error getting next run time: {str(e)}")
            return None

if __name__ == "__main__":
    scheduler = ScraperScheduler()
    scheduler.start()