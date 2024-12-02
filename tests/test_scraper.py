from src.scraper.convertkit_scraper import ConvertKitScraper
from dotenv import load_dotenv
import os

def test_full_scrape():
    """Test the full scraping process"""
    try:
        # Load environment variables from .env file
        load_dotenv()
        
        # Get credentials from environment variables
        email = os.getenv('CONVERTKIT_EMAIL')
        password = os.getenv('CONVERTKIT_PASSWORD')
        
        if not email or not password:
            raise ValueError("Please set CONVERTKIT_EMAIL and CONVERTKIT_PASSWORD environment variables")
        
        # Use the correct Chrome profile path
        chrome_profile = "/Users/shanemartin/Library/Application Support/Google/Chrome"
        
        print(f"\nUsing Chrome profile at: {chrome_profile}")
        
        # Initialize scraper with profile
        scraper = ConvertKitScraper(
            email=email,
            password=password,
            chrome_profile_path=chrome_profile
        )
        
        print("\nTesting login...")
        scraper.login()
        
        print("\nNavigating to Creator Network...")
        scraper.navigate_to_creator_network()
        
        print("\nStarting full account scraping process...")
        print("This will:")
        print("1. Get list of available accounts")
        print("2. Switch to each account")
        print("3. Navigate to Creator Network for each account")
        print("4. Scrape referral data from each account")
        print("5. Save data to CSV\n")
        
        data = scraper.scrape_all_accounts()
        
        print("\nData collection completed!")
        print(f"Collected data from {len(data)} accounts")
        print("\nAccounts processed:")
        for account_data in data:
            print(f"- {account_data['account_name']}")
            print(f"  Recommending me: {len(account_data['recommending_me'])} entries")
            print(f"  My recommendations: {len(account_data['my_recommendations'])} entries")
        
        # Optional: Add a pause to keep the window open
        input("\nPress Enter to close the browser...")
        
    except Exception as e:
        print(f"\nTest failed: {str(e)}")
        raise
    finally:
        if 'scraper' in locals():
            print("\nClosing browser...")
            scraper.driver.quit()

if __name__ == "__main__":
    test_full_scrape()