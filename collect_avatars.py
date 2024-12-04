from src.scraper.convertkit_scraper import ConvertKitScraper
from dotenv import load_dotenv

def main():
    load_dotenv()
    
    print("=== Starting Avatar Collection ===")
    
    scraper = ConvertKitScraper()
    
    try:
        # Just login and collect avatars
        scraper.login()
        scraper.scrape_partner_avatars()
        
    except Exception as e:
        print(f"\nError during collection: {str(e)}")
    finally:
        if hasattr(scraper, 'driver'):
            scraper.driver.quit()

if __name__ == "__main__":
    main() 