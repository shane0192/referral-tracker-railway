from apscheduler.schedulers.blocking import BlockingScheduler
from .convertkit_scraper import ConvertKitScraper
from datetime import datetime, timedelta
import logging
import json
import os
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScraperScheduler:
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.slack_webhook = "https://hooks.slack.com/services/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"
        self.last_run_file = 'last_run.json'
        
    def send_notification(self, message):
        """Send Slack notification"""
        try:
            payload = {"text": message}
            response = requests.post(self.slack_webhook, json=payload)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")

    def get_last_run(self):
        if os.path.exists(self.last_run_file):
            with open(self.last_run_file, 'r') as f:
                data = json.load(f)
                return datetime.fromisoformat(data['last_run'])
        return None
        
    def save_last_run(self):
        with open(self.last_run_file, 'w') as f:
            json.dump({'last_run': datetime.now().isoformat()}, f)
            
    def should_run(self):
        # Original logic
        # if self.last_run and (datetime.now() - self.last_run).total_seconds() < self.min_interval:
        #     logging.info("Skipping run - too soon since last successful run")
        #     return False
        
        # Force run
        return True

    def run_scraper(self):
        """Run the scraper if needed"""
        if not self.should_run():
            logger.info("Skipping run - too soon since last successful run")
            return
            
        try:
            # Create scraper instance without parameters
            scraper = ConvertKitScraper()
            
            if not scraper.login():
                self.send_notification("🚨 ConvertKit login needed - please log in manually")
                return

            scraper.navigate_to_creator_network()
            accounts = scraper.get_available_accounts()
            
            success = True
            failed_accounts = []
            
            for account in accounts:
                try:
                    scraper.switch_to_account(account)
                    scraper.scrape_referral_data()
                except Exception as e:
                    success = False
                    failed_accounts.append(account['name'])

            if success:
                self.save_last_run()
                self.send_notification("✅ ConvertKit data successfully collected for today")
            else:
                failed_list = ", ".join(failed_accounts)
                self.send_notification(f"⚠️ ConvertKit data collection partially failed. Problem accounts: {failed_list}")
            
        except Exception as e:
            self.send_notification(f"❌ ConvertKit scraper error: {str(e)}")
        finally:
            if 'scraper' in locals():
                scraper.close()

    def start(self):
        """Start the scheduler"""
        self.run_scraper()
        
        # Check every hour if we need to run
        self.scheduler.add_job(
            self.run_scraper,
            'interval',
            hours=1,
            next_run_time=datetime.now() + timedelta(hours=1)
        )
        logger.info("Scheduler started")
        self.scheduler.start()

if __name__ == "__main__":
    scheduler = ScraperScheduler()
    scheduler.start()