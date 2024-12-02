from src.scraper.convertkit_scraper import ConvertKitScraper
from src.data.db_manager import DatabaseManager
import os
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    
    print("=== Starting Full Test ===")
    
    # Initialize scraper and database
    print("\n1. Initializing components...")
    scraper = ConvertKitScraper()
    db = DatabaseManager()
    
    try:
        # Test scraping
        print("\n2. Testing scraping functionality...")
        print("- Logging in...")
        scraper.login()
        
        print("- Getting available accounts...")
        accounts = scraper.get_available_accounts()
        
        # Process each account
        for account in accounts:
            print(f"\n- Processing account: {account['name']}")
            scraper.switch_to_account(account)
            
            print("- Navigating to creator network...")
            scraper.navigate_to_creator_network()
            
            print("- Scraping referral data...")
            scraper.scrape_referral_data()
        
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
        print(f"\nError during test: {str(e)}")
        if hasattr(scraper, 'driver'):
            print("\nCurrent URL:", scraper.driver.current_url)

if __name__ == "__main__":
    main()