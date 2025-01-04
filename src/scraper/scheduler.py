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
import pytz

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
    def __init__(self, enabled_accounts=None):
        self.scraper = ConvertKitScraper(headless=True)
        self.enabled_accounts = enabled_accounts or []
        self.slack_webhook = "https://hooks.slack.com/services/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"
        self.last_run_file = 'last_run.json'
        self.max_retries = 3
        self.retry_delay = 5
        self.pending_accounts = set()
        self.successful_accounts = set()
        self.timezone = pytz.timezone('America/Los_Angeles')
        
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
                # Convert current time to PT
                pt_now = datetime.now(self.timezone)
                json.dump({
                    'last_run': pt_now.isoformat(),
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
            logger.info("Starting scraper run")
            
            # Load enabled accounts from config
            config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config')
            config_file = os.path.join(config_path, 'enabled_accounts.json')
            try:
                with open(config_file, 'r') as f:
                    config_data = json.load(f)
                    if isinstance(config_data, list):
                        enabled_accounts = config_data
                    else:
                        enabled_accounts = config_data.get('enabled', [])
            except (FileNotFoundError, json.JSONDecodeError):
                enabled_accounts = []
            
            logger.info(f"Enabled accounts: {enabled_accounts}")
            
            # Get last run state
            last_run = self.get_last_run()
            
            # Determine which accounts to process
            target_accounts = set(enabled_accounts)  # Only process enabled accounts
            if not force:
                # Remove accounts that were successful today
                target_accounts -= self.successful_accounts
            
            if not target_accounts:
                logger.info("No accounts to process")
                return True
            
            logger.info(f"Processing accounts: {target_accounts}")
            
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
                    
                    # Try headless first, then visible if that fails
                    try:
                        scraper = ConvertKitScraper(headless=True)
                    except Exception as e:
                        logger.info("Headless mode failed, trying visible mode...")
                        scraper = ConvertKitScraper(headless=False)
                    
                    # If login fails, restart with visible browser
                    if not scraper.login():
                        logger.info("Login failed - restarting with visible browser for manual login")
                        scraper.driver.quit()
                        scraper = ConvertKitScraper(headless=False)
                        self.send_notification("🚨 ConvertKit login needed - please log in manually")
                        return False

                    success = True
                    failed_accounts = []
                    
                    accounts = scraper.get_available_accounts()
                    for account in accounts:
                        # Only process accounts that are in our target set
                        if account['name'] not in target_accounts:
                            continue
                            
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

                    # Update account states
                    for account in accounts:
                        if account['name'] in target_accounts:
                            if account['name'] not in failed_accounts:
                                self.successful_accounts.add(account['name'])
                                self.pending_accounts.discard(account['name'])
                            else:
                                self.pending_accounts.add(account['name'])

                    # Save the state
                    self.save_last_run()
                    
                    # Send appropriate notification
                    if not failed_accounts:
                        self.send_notification("✅ ConvertKit data successfully collected for today")
                        return True
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
                            pass

            return False

        except Exception as e:
            logger.error(f"Critical scheduler error: {str(e)}", exc_info=True)
            self.send_notification(f"🚨 Critical scheduler error: {str(e)}")
            raise

if __name__ == "__main__":
    scheduler = ScraperScheduler()
    scheduler.run_scraper(force=True)