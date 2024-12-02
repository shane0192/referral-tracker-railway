from src.data.db_manager import DatabaseManager
from src.scraper.convertkit_scraper import ConvertKitScraper

def main():
    # List of accounts to scrape
    accounts = [
        "The Perfect Loaf",
        "Account 2",
        "Account 3",
        # Add all your accounts here
    ]
    
    # First, scrape new data
    scraper = ConvertKitScraper()
    scraper.scrape_all_accounts(accounts)

    # Initialize database manager
    db = DatabaseManager()
    
    # Import the CSV data
    print("Importing CSV data...")
    db.import_csv()
    
    # Then check if data was imported successfully
    data = db.get_latest_data()
    if not data:
        print("No data found in database!")
    else:
        print(f"Found {len(data)} records:")
        for record in data:
            print(f"Account: {record.account_name}")
            print(f"Date: {record.date}")
            print("---")
        
        # Create the HTML viewer
        print("Creating HTML viewer...")
        db.create_html_viewer()

if __name__ == "__main__":
    main() 