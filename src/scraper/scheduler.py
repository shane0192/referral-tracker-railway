from apscheduler.schedulers.blocking import BlockingScheduler
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.scraper.convertkit_scraper import ConvertKitScraper
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
        
    def send_notification(self, message):
        """Send Slack notification"""
        try:
            payload = {"text": message}
            response = requests.post(self.slack_webhook, json=payload)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")

    def get_last_run(self):
        """Get the last successful run time"""
        try:
            if os.path.exists(self.last_run_file):
                with open(self.last_run_file, 'r') as f:
                    data = json.load(f)
                    if data and 'last_run' in data:
                        # Convert the ISO string to datetime
                        return datetime.fromisoformat(data['last_run'])
            return None
        except Exception as e:
            logger.error(f"Error reading last run time: {str(e)}")
            return None
        
    def save_last_run(self):
        with open(self.last_run_file, 'w') as f:
            json.dump({'last_run': datetime.now().isoformat()}, f)
            
    def should_run(self, force=False):
        """Check if scraper should run"""
        if force:
            logger.info("Force run requested - ignoring last run time")
            return True
        
        try:
            with open('last_successful_run.txt', 'r') as f:
                last_run = datetime.fromisoformat(f.read().strip())
                next_run = last_run.replace(hour=6, minute=0, second=0, microsecond=0)
                if last_run.hour >= 6:
                    next_run += timedelta(days=1)
                
                logger.info(f"Last successful run: {last_run}")
                logger.info(f"Next scheduled run: {next_run}")
                
                if datetime.now() < next_run:
                    logger.info("Skipping run - too soon since last successful run")
                    return False
                
            return True
        except FileNotFoundError:
            logger.info("No previous run found")
            return True

    def run_scraper(self, force=False):
        """Run the scraper"""
        logger.info("Running scraper...")
        if not self.should_run(force=force):
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
                
                # Start with headless mode
                scraper = ConvertKitScraper(headless=True)
                
                # If login fails, restart with visible browser
                if not scraper.login():
                    logger.info("Login failed - restarting with visible browser for manual login")
                    scraper.driver.quit()
                    scraper = ConvertKitScraper(headless=False)
                    self.send_notification("🚨 ConvertKit login needed - please log in manually")
                    return

                success = True
                failed_accounts = []
                
                accounts = scraper.get_available_accounts()
                for account in accounts:
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

    def start(self):
        """Start the scheduler"""
        try:
            # Schedule daily run at 6 AM
            self.scheduler.add_job(
                self.run_scraper,
                'cron',
                hour=6,
                minute=0,
                id='daily_scrape'
            )
            
            # Add a backup hourly check
            self.scheduler.add_job(
                self.run_scraper,
                'interval',
                hours=1,
                id='hourly_check'
            )
            
            logger.info("Scheduler started successfully")
            
            # Start the scheduler first
            self.scheduler.start()
            
            # Then get the next run times
            logger.info(f"Next scheduled runs:")
            for job in self.scheduler.get_jobs():
                logger.info(f"- {job.id}: {job.next_run_time}")
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {str(e)}")
            raise

    def get_next_run_time(self):
        """Get the next scheduled run time"""
        jobs = self.scheduler.get_jobs()
        if jobs:
            next_run = jobs[0].next_run_time
            logger.info(f"Next scheduled run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            return next_run
        return None

if __name__ == "__main__":
    scheduler = ScraperScheduler()
    scheduler.start()