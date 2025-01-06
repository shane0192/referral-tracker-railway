from src.scraper.convertkit_scraper import ConvertKitScraper
from src.data.db_manager import DatabaseManager
import os
from dotenv import load_dotenv
import traceback
import time

def main():
    # Load environment variables
    load_dotenv()
    
    print("=== Starting Full Test ===")
    
    # Initialize scraper and database
    print("\n1. Initializing components...")
    scraper = ConvertKitScraper(headless=False)  # Start in visible mode for debugging
    db = DatabaseManager()
    
    try:
        # Test scraping
        print("\n2. Testing scraping functionality...")
        print("- Logging in...")
        if not scraper.login():
            print("❌ Login failed. Please complete the login process manually.")
            return
            
        # Wait a bit after login to ensure page is loaded
        time.sleep(5)
        
        print("- Getting available accounts...")
        try:
            accounts = scraper.get_available_accounts()
            if not accounts:
                print("❌ No accounts found. Please check if you're properly logged in.")
                return
        except Exception as e:
            print(f"❌ Error getting accounts: {str(e)}")
            print("Current URL:", scraper.driver.current_url)
            print("Stack trace:", traceback.format_exc())
            return
        
        # Process each account
        for account in accounts:
            try:
                print(f"\n- Processing account: {account['name']}")
                scraper.switch_to_account(account)
                
                print("- Navigating to creator network...")
                scraper.navigate_to_creator_network()
                
                print("- Scraping referral data...")
                scraper.scrape_referral_data()
            except Exception as e:
                print(f"❌ Error processing account {account['name']}: {str(e)}")
                print("Current URL:", scraper.driver.current_url)
                continue
        
        # Test database operations
        print("\n3. Testing database operations...")
        print("- Importing CSV data...")
        db.import_csv()
        
        print("- Checking imported data...")
        data = db.get_latest_data()
        if data:
            print(f"Successfully imported {len(data)} records")
            
        # Test HTML viewer creation
        print("\n4. Testing HTML viewer creation...")
        viewer_path = db.create_html_viewer()
        print(f"HTML viewer created at: {viewer_path}")
        
        print("\n=== Test Completed Successfully! ===")
        
    except Exception as e:
        print(f"\n❌ Error during test: {str(e)}")
        print("\nStack trace:")
        print(traceback.format_exc())
        if hasattr(scraper, 'driver'):
            print("\nCurrent URL:", scraper.driver.current_url)
    finally:
        if hasattr(scraper, 'driver'):
            try:
                scraper.driver.quit()
            except:
                pass

if __name__ == "__main__":
    main()