from src.scraper.convertkit_scraper import ConvertKitScraper
from src.scraper.scheduler import ScraperScheduler
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("Starting manual scrape test...")
    try:
        # Initialize scraper in headless mode
        scraper = ConvertKitScraper(headless=True)
        
        # Initialize scheduler for tracking successful accounts
        scheduler = ScraperScheduler()
        scheduler.get_last_run()  # Load previously successful accounts
        
        # Check login status
        print("\nVerifying login status...")
        is_logged_in = scraper.login()
        
        if is_logged_in:
            print("✅ Successfully verified login - session is active")
            
            # Get available accounts
            print("\nGetting available accounts...")
            accounts = scraper.get_available_accounts()
            
            # Process each account
            for account in accounts:
                try:
                    # Skip if already successful today
                    if account['name'] in scheduler.successful_accounts:
                        print(f"\nSkipping {account['name']} - already successful today")
                        continue
                        
                    print(f"\nProcessing account: {account['name']}")
                    scraper.switch_to_account(account)
                    data = scraper.scrape_referral_data()
                    print(f"✅ Successfully scraped data for {account['name']}")
                    
                    # Mark as successful and save state
                    scheduler.successful_accounts.add(account['name'])
                    scheduler.save_last_run()
                    
                except Exception as e:
                    print(f"❌ Error processing {account['name']}: {str(e)}")
                    scheduler.pending_accounts.add(account['name'])
                    scheduler.save_last_run()
                    
        else:
            print("❌ Login failed - you may need to re-authenticate")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if scraper:
            scraper.driver.quit()
