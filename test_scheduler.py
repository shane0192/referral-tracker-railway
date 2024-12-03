from src.scraper.scheduler import ScraperScheduler
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("Starting scheduler test...")
    scheduler = ScraperScheduler()
    
    # This will run the scraper immediately and then schedule future runs
    scheduler.run_scraper() 