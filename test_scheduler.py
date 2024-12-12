from src.scraper.convertkit_scraper import ConvertKitScraper
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("Starting manual scrape test...")
    try:
        # Initialize scraper in headless mode
        scraper = ConvertKitScraper(headless=True)
        
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
                    print(f"\nProcessing account: {account['name']}")
                    scraper.switch_to_account(account)
                    data = scraper.scrape_referral_data()
                    print(f"✅ Successfully scraped data for {account['name']}")
                except Exception as e:
                    print(f"❌ Error processing {account['name']}: {str(e)}")
                    
        else:
            print("❌ Login failed - you may need to re-authenticate")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if scraper:
            scraper.driver.quit()