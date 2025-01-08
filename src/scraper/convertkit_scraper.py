import sys
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import logging
import time
import pickle
import pandas as pd

# Keep your existing path setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# Update these imports to match your actual file structure
from src.data.db_manager import DatabaseManager, ReferralData
from src.utils.config import CONVERTKIT_EMAIL, CONVERTKIT_PASSWORD

class ConvertKitScraper:
    def __init__(self, headless=True):
        """Initialize the scraper with login credentials from config"""
        self.email = CONVERTKIT_EMAIL
        self.password = CONVERTKIT_PASSWORD
        self.headless = True  # Always use headless mode
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        self.is_heroku = 'DYNO' in os.environ
        
        # Initialize Chrome driver based on environment
        self.setup_chrome_driver()
        
    def setup_chrome_driver(self):
        """Set up Chrome driver with appropriate configuration for environment"""
        try:
            chrome_options = Options()
            
            if self.is_heroku:
                self._setup_heroku_options(chrome_options)
            else:
                self._setup_local_options(chrome_options)
            
            # Common options for both environments
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # Initialize the driver
            if self.is_heroku:
                chromedriver_path = os.environ.get('CHROMEDRIVER_PATH')
                if not chromedriver_path:
                    raise Exception("ChromeDriver path not found in Heroku environment")
            else:
                chromedriver_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                    'chrome-for-testing/chromedriver-mac-arm64/chromedriver'
                )
            
            print(f"\nInitializing Chrome driver...")
            print(f"Environment: {'Heroku' if self.is_heroku else 'Local'}")
            print(f"ChromeDriver path: {chromedriver_path}")
            
            service = webdriver.ChromeService(executable_path=chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            print("✅ Chrome driver initialized successfully")
            
        except Exception as e:
            print(f"❌ Failed to initialize Chrome driver: {str(e)}")
            if self.is_heroku:
                print("\nHeroku environment variables:")
                print(f"GOOGLE_CHROME_BIN: {os.environ.get('GOOGLE_CHROME_BIN')}")
                print(f"GOOGLE_CHROME_SHIM: {os.environ.get('GOOGLE_CHROME_SHIM')}")
                print(f"CHROMEDRIVER_PATH: {os.environ.get('CHROMEDRIVER_PATH')}")
            raise
            
    def _setup_heroku_options(self, chrome_options):
        """Configure Chrome options for Heroku environment"""
        # Get Chrome binary path
        chrome_binary = os.environ.get('GOOGLE_CHROME_SHIM') or os.environ.get('GOOGLE_CHROME_BIN')
        if not chrome_binary:
            raise Exception("Chrome binary path not found in Heroku environment")
            
        chrome_options.binary_location = chrome_binary
        
        # Heroku-specific options
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--disable-setuid-sandbox')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--single-process')
        
        # Memory optimizations for Heroku
        chrome_options.add_argument('--js-flags=--max-old-space-size=2048')
        chrome_options.add_argument('--memory-pressure-off')
        
    def _setup_local_options(self, chrome_options):
        """Configure Chrome options for local environment"""
        # Set up profile directory for session persistence
        self.profile_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'automation_chrome_profile'
        )
        
        if not os.path.exists(self.profile_dir):
            os.makedirs(self.profile_dir)
            print("\n⚠️ First time setup: You'll need to log in manually once and verify 2FA")
            print("After this, the session should persist for about a month")
            self.headless = False  # Force visible mode only for first-time setup
            
        print(f"Using Chrome profile at: {self.profile_dir}")
        
        # Local-specific options
        chrome_options.add_argument(f'user-data-dir={self.profile_dir}')
        chrome_options.add_argument('--profile-directory=Default')
        
        if self.headless:
            chrome_options.add_argument('--headless=new')
            
        # Additional stability options for local
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

    def save_cookies(self):
        """Save cookies after successful login"""
        print("Saving cookies...")
        with open(self.cookies_file, 'wb') as f:
            pickle.dump(self.driver.get_cookies(), f)
        print("Cookies saved successfully!")

    def load_cookies(self):
        """Load saved cookies if they exist"""
        try:
            if os.path.exists(self.cookies_file):
                print("Loading saved cookies...")
                with open(self.cookies_file, 'rb') as f:
                    cookies = pickle.load(f)
                    # First access the site
                    self.driver.get(CONVERTKIT_LOGIN_URL)
                    for cookie in cookies:
                        self.driver.add_cookie(cookie)
                print("Cookies loaded successfully!")
                return True
            return False
        except Exception as e:
            print(f"Error loading cookies: {str(e)}")
            return False

    def login(self):
        """Login to Kit.com with clear notifications for manual intervention"""
        try:
            print("Checking login status...")
            self.driver.get("https://app.kit.com/dashboard")
            time.sleep(5)
            
            current_url = self.driver.current_url
            print(f"Current URL: {current_url}")
            
            if "dashboard" in current_url or "/creator-network" in current_url:
                print("✅ Already logged in!")
                return True
            
            print("\n🚨 LOGIN REQUIRED 🚨")
            print("Session has expired and manual login is needed.")
            
            # If we're in headless mode, restart in non-headless mode
            if self.headless:
                print("Restarting browser in non-headless mode for manual login...")
                self.driver.quit()
                self.headless = False
                
                # Reinitialize the driver with updated options
                chrome_options = Options()
                chrome_options.add_argument(f'user-data-dir={self.profile_dir}')
                chrome_options.add_argument('--profile-directory=Default')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--window-size=1920,1080')
                
                chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/app/.chrome-for-testing/chromedriver-linux64/chromedriver')
                service = webdriver.ChromeService(executable_path=chromedriver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                
                # Navigate to login page again
                self.driver.get("https://app.kit.com/dashboard")
            
            print("\nNext steps:")
            print("1. Log in manually in the browser window")
            print("2. Complete 2FA verification if prompted")
            print("3. The script will wait up to 5 minutes for login completion")
            
            # Wait for manual login
            max_wait = 300  # 5 minutes
            start_time = time.time()
            
            while time.time() - start_time < max_wait:
                remaining = int(max_wait - (time.time() - start_time))
                sys.stdout.write(f"\rWaiting for manual login... {remaining} seconds remaining")
                sys.stdout.flush()
                
                # Check if we're logged in
                if "dashboard" in self.driver.current_url:
                    print("\n✅ Login successful!")
                    return True
                    
                time.sleep(1)
            
            print("\n❌ Login timeout - please run the script again when you can complete the login process")
            self.driver.save_screenshot("login_timeout.png")
            return False
            
        except Exception as e:
            print(f"\n❌ Login error: {str(e)}")
            self.driver.save_screenshot("login_error.png")
            raise

    def navigate_to_dashboard(self):
        """Navigate to the Creator Network dashboard"""
        try:
            print(f"Navigating to Creator Network: {CONVERTKIT_DASHBOARD_URL}")
            self.driver.get(CONVERTKIT_DASHBOARD_URL)
            time.sleep(3)
            
            # Take screenshot
            self.driver.save_screenshot("creator_network.png")
            print("Current URL:", self.driver.current_url)
            
        except Exception as e:
            print(f"Failed to navigate to Creator Network: {str(e)}")
            self.driver.save_screenshot("navigation_error.png")
            raise

    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_driver(self):
        """Create and return a new WebDriver instance with retries"""
        for attempt in range(self.max_retries):
            try:
                # Setup Chrome options
                chrome_options = Options()
                chrome_options.add_argument(f'user-data-dir={self.profile_dir}')
                chrome_options.add_argument('--profile-directory=Default')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--window-size=1920,1080')
                
                print(f"\nInitializing Chrome driver (attempt {attempt + 1}/{self.max_retries})...")
                driver = webdriver.Chrome(options=chrome_options)
                print("Chrome driver initialized successfully")
                return driver
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                print(f"Failed to create driver (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                time.sleep(self.retry_delay)
                
    def restart_session(self):
        """Restart the browser session with proper cleanup"""
        try:
            if self.driver:
                print("Closing existing browser session...")
                try:
                    self.driver.quit()
                except:
                    pass  # Ignore cleanup errors
                
            print("Creating new browser session...")
            self.driver = self.create_driver()
            
            if not self.driver:
                print("Failed to create new driver")
                return False
                
            print("Attempting to log in...")
            if not self.login():
                print("Failed to log in after restart")
                return False
                
            print("Session successfully restarted")
            return True
            
        except Exception as e:
            print(f"Failed to restart session: {str(e)}")
            return False

    def validate_scrape_data(self, data, account_name):
        """Validate scraped data for completeness and compare with historical data"""
        if not data:
            message = f"❌ No data retrieved for {account_name}"
            print(message)
            return False, message
        
        recommending_count = len(data.get('recommending_me', []))
        recommendations_count = len(data.get('my_recommendations', []))
        
        print(f"\n=== Data Validation for {account_name} ===")
        print(f"Current recommending me entries: {recommending_count}")
        print(f"Current recommendations entries: {recommendations_count}")
        
        # Get previous day's data
        db = DatabaseManager()
        session = db.Session()
        try:
            previous_record = session.query(ReferralData)\
                .filter(ReferralData.account_name == account_name)\
                .order_by(ReferralData.date.desc())\
                .first()
            
            if previous_record:
                prev_recommending = len(previous_record.recommending_me)
                prev_recommendations = len(previous_record.my_recommendations)
                
                # Check for significant drops (more than 3 entries)
                MAX_ALLOWED_DROP = 3
                
                if (prev_recommending - recommending_count) > MAX_ALLOWED_DROP:
                    message = (f"⚠️ Suspicious drop in recommending_me for {account_name}:\n"
                              f"  Previous: {prev_recommending}\n"
                              f"  Current: {recommending_count}\n"
                              f"  Drop: {prev_recommending - recommending_count}")
                    print(message)
                    return False, message
                    
                if (prev_recommendations - recommendations_count) > MAX_ALLOWED_DROP:
                    message = (f"⚠️ Suspicious drop in recommendations for {account_name}:\n"
                              f"  Previous: {prev_recommendations}\n"
                              f"  Current: {recommendations_count}\n"
                              f"  Drop: {prev_recommendations - recommendations_count}")
                    print(message)
                    return False, message
                    
                print(f"Historical comparison for {account_name}:")
                print(f"  Previous recommending: {prev_recommending}")
                print(f"  Previous recommendations: {prev_recommendations}")
                
        finally:
            session.close()
        
        # Continue with minimum threshold validation
        if account_name == "Chris Donnelly":
            MIN_RECOMMENDING_ME = 10
            MIN_RECOMMENDATIONS = 10
        else:
            MIN_RECOMMENDING_ME = 5
            MIN_RECOMMENDATIONS = 5
        
        is_complete = (
            recommending_count >= MIN_RECOMMENDING_ME or 
            recommendations_count >= MIN_RECOMMENDATIONS
        )
        
        if not is_complete:
            message = (f"❌ Incomplete data for {account_name}:\n"
                      f"  Expected min {MIN_RECOMMENDING_ME} recommending, got {recommending_count}\n"
                      f"  Expected min {MIN_RECOMMENDATIONS} recommendations, got {recommendations_count}")
            print(message)
            return False, message
        
        message = f"✅ Data validation passed for {account_name}"
        print(message)
        return True, message

    def scrape_referral_data(self):
        """Scrape referral data with validation"""
        for attempt in range(self.max_retries):
            try:
                data = {
                    'date': datetime.now(),
                    'recommending_me': [],
                    'my_recommendations': []
                }
                
                """Scrape referral data for current account"""
                if not self.current_account:
                    print("No account selected!")
                    return
                
                try:
                    # First ensure we're on the creator network page
                    print("\nEnsuring we're on the Creator Network page...")
                    self.driver.get("https://app.kit.com/creator-network")
                    time.sleep(5)  # Wait for navigation
                    
                    # Verify we're on the correct page
                    current_url = self.driver.current_url
                    print(f"Current URL: {current_url}")
                    if "creator-network" not in current_url:
                        print("⚠️ Not on Creator Network page! Attempting to navigate...")
                        self.navigate_to_creator_network()
                        time.sleep(5)  # Additional wait after navigation
                    
                    print("Starting data collection...")
                    data = {
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'recommending_me': [],
                        'my_recommendations': []
                    }
                    
                    # First get data from the default "Recommending me" tab
                    print("\nScraping data from default 'Recommending me' tab...")
                    data['recommending_me'] = self.get_table_data(tab_type="recommending_me")
                    print(f"Found {len(data['recommending_me'])} entries in first tab")
                    
                    # Take screenshot to debug
                    self.driver.save_screenshot("before_tab_switch.png")
                    
                    # Now find and click the "My Recommendations" tab
                    print("\nLooking for 'My Recommendations' tab...")
                    try:
                        # Try different possible selectors for the My Recommendations tab
                        selectors = [
                            "//a[contains(text(), 'My Recommendations')]",
                            "//button[contains(text(), 'My Recommendations')]",
                            "//div[contains(text(), 'My Recommendations')]",
                            "//span[contains(text(), 'My Recommendations')]"
                        ]
                        
                        tab_found = False
                        for selector in selectors:
                            try:
                                print(f"Trying selector: {selector}")
                                my_recommendations_tab = WebDriverWait(self.driver, 5).until(
                                    EC.element_to_be_clickable((By.XPATH, selector))
                                )
                                print("Found tab! Clicking...")
                                my_recommendations_tab.click()
                                tab_found = True
                                break
                            except Exception as e:
                                print(f"Selector failed: {str(e)}")
                                continue
                        
                        if not tab_found:
                            raise Exception("Could not find 'My Recommendations' tab")
                        
                        # Wait for table to update
                        time.sleep(3)
                        
                        print("Waiting for table data to load...")
                        time.sleep(5)  # Add explicit wait after tab switch
                        
                        print("Scraping data from 'My Recommendations' tab...")
                        data['my_recommendations'] = self.get_table_data(tab_type="my_recommendations")
                        print(f"Found {len(data['my_recommendations'])} entries in second tab")
                        
                    except Exception as e:
                        print(f"Error with second tab: {str(e)}")
                        self.driver.save_screenshot("second_tab_error.png")
                    
                    # Print data clearly
                    print("\n=== Scraped Data ===")
                    print(f"\nDate: {data['date']}")
                    print("\nRecommending Me Tab:")
                    for row in data['recommending_me']:
                        print(f"  {row['creator']} - Subscribers: {row['subscribers']}, Conversion: {row['conversion_rate']}")
                    print("\nMy Recommendations Tab:")
                    for row in data['my_recommendations']:
                        print(f"  {row['creator']} - Subscribers: {row['subscribers']}, Conversion: {row['conversion_rate']}")
                    
                    # Add validation before saving
                    if not self.validate_scrape_data(data, self.current_account):
                        raise Exception(f"Data validation failed for {self.current_account}")
                    
                    # Save data only if validation passes
                    self.save_referral_data(
                        account_name=self.current_account,
                        recommending_me_data=data['recommending_me'],
                        my_recommendations_data=data['my_recommendations']
                    )
                    
                    db = DatabaseManager()
                    db.save_data(
                        account_name=self.current_account,
                        recommending_me=data['recommending_me'],
                        my_recommendations=data['my_recommendations']
                    )
                    
                    return data
                    
                except Exception as e:
                    print(f"Failed to scrape referral data: {str(e)}")
                    self.driver.save_screenshot("scraping_error.png")
                    raise
            except Exception as e:
                self.save_screenshot(f"scraping_error_{attempt}.png")
                print(f"Scraping attempt {attempt + 1}/{self.max_retries} failed: {str(e)}")
                
                if attempt == self.max_retries - 1:
                    raise
                
                print("Attempting to restart session...")
                if not self.restart_session():
                    print("Failed to restart session")
                    raise
                time.sleep(self.retry_delay)

    def get_table_data(self, tab_type="my_recommendations", max_retries=3):
        """Helper method to get specific columns from the table"""
        for attempt in range(max_retries):
            try:
                print(f"\nAttempt {attempt + 1} of {max_retries}")
                print(f"Current URL: {self.driver.current_url}")
                
                # Take screenshot for debugging
                screenshot_name = f"table_attempt_{attempt + 1}.png"
                self.driver.save_screenshot(screenshot_name)
                
                # Wait longer initially
                print("Initial wait...")
                time.sleep(15)
                
                # Try different table selectors
                selectors = [
                    "table",  # Basic table
                    "//table",  # XPath table
                    "//div[contains(@class, 'table')]",  # Div that might be styled as table
                    "//div[contains(@role, 'table')]"  # ARIA table role
                ]
                
                table = None
                for selector in selectors:
                    try:
                        print(f"\nTrying selector: {selector}")
                        if selector.startswith("//"):
                            table = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, selector))
                            )
                        else:
                            table = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                        print("Found table!")
                        break
                    except Exception as e:
                        print(f"Selector failed: {str(e)}")
                        continue
                
                if not table:
                    raise Exception("Could not find table with any selector")
                
                # Get rows
                print("\nLooking for rows...")
                rows = table.find_elements(By.TAG_NAME, "tr")
                print(f"Found {len(rows)} rows")
                
                data = []
                for row in rows:
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) >= 4:  # Ensure there are enough cells
                            creator = cells[0].text.strip()
                            subscribers = cells[2].text.strip()  # Adjusted index for subscribers
                            conversion = cells[3].text.strip()  # Adjusted index for conversion rate
                            
                            if creator and subscribers and conversion:
                                data.append({
                                    'creator': creator,
                                    'subscribers': subscribers,
                                    'conversion_rate': conversion
                                })
                                print(f"Added row: {creator} - {subscribers} - {conversion}")
                    except Exception as e:
                        print(f"Error processing row: {str(e)}")
                        continue
                
                if data:
                    return data
                
                print("No data found in table, retrying...")
                time.sleep(10)  # Wait before retry
                
            except Exception as e:
                print(f"Failed to get table data: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"Retrying... (Attempt {attempt + 1})")
                    time.sleep(10 * (attempt + 1))
                    continue
                return []

    def save_referral_data(self, account_name, recommending_me_data, my_recommendations_data):
        """Save referral data to CSV with account name"""
        try:
            # Get the absolute path to the project root
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            
            # Create the absolute path to the CSV file
            csv_path = os.path.join(project_root, 'src', 'data', 'referral_data.csv')
            
            print(f"Using absolute path: {csv_path}")
            
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            rows = []
            
            # Add recommending_me data
            for item in recommending_me_data:
                rows.append({
                    'date': current_time,
                    'account_name': account_name,
                    'tab': 'recommending_me',
                    'creator': item['creator'],
                    'subscribers': item['subscribers'],
                    'conversion_rate': item['conversion_rate']
                })
            
            # Add my_recommendations data
            for item in my_recommendations_data:
                rows.append({
                    'date': current_time,
                    'account_name': account_name,
                    'tab': 'my_recommendations',
                    'creator': item['creator'],
                    'subscribers': item['subscribers'],
                    'conversion_rate': item['conversion_rate']
                })

            # Create DataFrame
            df = pd.DataFrame(rows)
            
            # Add debug prints here
            print(f"Saving {len(rows)} rows to {csv_path}")
            print(f"File exists: {os.path.exists(csv_path)}")
            print(f"Will write headers: {not os.path.exists(csv_path)}")
            
            # Simple save - if file exists append without header, if not create with header
            df.to_csv(csv_path, 
                      mode='a',
                      header=not os.path.exists(csv_path),
                      index=False)
            
            print(f"Data saved for account: {account_name}")
            
        except Exception as e:
            print(f"Error saving data for {account_name}: {e}")
            print(f"Current working directory: {os.getcwd()}")  # Debug print

    def get_available_accounts(self):
        """Get list of available accounts from dropdown"""
        try:
            print("Getting list of available accounts...")
            
            # Click the account menu button to open dropdown
            print("Looking for account menu button...")
            menu_selector = "//button[@aria-haspopup='true' and contains(@class, 'inline-flex')]"
            menu_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, menu_selector))
            )
                
            print("Found menu button, clicking...")
            menu_button.click()
            time.sleep(2)
            
            # Get all account elements
            print("Looking for account elements...")
            account_elements = WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((
                    By.XPATH, 
                    "//a[@role='menuitem' and contains(@class, 'settings-link')]"
                ))
            )
            
            print(f"Found {len(account_elements)} potential account elements")
            
            accounts = []
            for element in account_elements:
                try:
                    # Get the duplicated name
                    account_name = element.get_attribute('data-valuetext')
                    if account_name and account_name != "Settings" and account_name != "Log out":
                        # Clean the duplicated name (e.g., "Adam GrahamAdam Graham" -> "Adam Graham")
                        name_length = len(account_name)
                        clean_name = account_name[:name_length//2]
                        
                        # Add all accounts without restriction
                        accounts.append({
                            'name': clean_name,
                            'email': element.get_attribute('data-account-email'),
                            'element': element
                        })
                        print(f"Added account: {clean_name}")
                except Exception as e:
                    print(f"Error processing element: {str(e)}")
                    continue
            
            if not accounts:
                print("\n🚨 No accounts found in dropdown!")
                print("Taking screenshot for debugging...")
                self.driver.save_screenshot("no_accounts_found.png")
                raise Exception("No accounts found in dropdown")
            
            print(f"\n✅ Successfully found {len(accounts)} accounts: {', '.join(acc['name'] for acc in accounts)}")
            return accounts
            
        except Exception as e:
            print(f"\n❌ Error getting available accounts: {str(e)}")
            self.driver.save_screenshot("get_accounts_error.png")
            raise

    def switch_to_account(self, account_info):
        """Switch to a different account with retries and improved error handling"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"\nAttempting to switch to account: {account_info['name']} (Attempt {attempt + 1}/{max_retries})")
                
                # First verify we're not already on this account
                current = self.get_current_account()
                if current == account_info['name']:
                    print(f"Already on account {account_info['name']}")
                    return True
                
                # Click account menu with retry
                menu_selectors = [
                    "//button[contains(@class, 'inline-flex') and .//img[@alt[contains(., 'Avatar')]]]",
                    "//button[@aria-haspopup='true']",
                    "//button[contains(@class, 'inline-flex')]//span[contains(@class, 'truncate')]/.."
                ]
                
                menu_clicked = False
                for selector in menu_selectors:
                    try:
                        menu_button = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, selector))
                        )
                        menu_button.click()
                        menu_clicked = True
                        break
                    except:
                        continue
                
                if not menu_clicked:
                    raise Exception("Could not click account menu with any selector")
                
                time.sleep(2)  # Wait for dropdown to appear
                
                # Find and click account link with retry
                account_selectors = [
                    f"//a[@role='menuitem']//div[contains(text(), '{account_info['name']}')]",
                    f"//a[@role='menuitem'][contains(@data-valuetext, '{account_info['name']}')]",
                    f"//a[contains(@class, 'settings-link')][contains(., '{account_info['name']}')]"
                ]
                
                account_clicked = False
                for selector in account_selectors:
                    try:
                        account_link = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, selector))
                        )
                        account_link.click()
                        account_clicked = True
                        break
                    except:
                        continue
                
                if not account_clicked:
                    raise Exception(f"Could not find account {account_info['name']} in dropdown")
                
                # Wait for switch and verify
                time.sleep(5)
                new_account = self.get_current_account()
                if new_account != account_info['name']:
                    raise Exception(f"Account switch failed. Expected {account_info['name']}, got {new_account}")
                
                print(f"✅ Successfully switched to account: {account_info['name']}")
                return True
                
            except Exception as e:
                print(f"Switch attempt {attempt + 1} failed: {str(e)}")
                self.driver.save_screenshot(f"switch_error_{attempt + 1}.png")
                
                if attempt == max_retries - 1:
                    print(f"❌ Failed to switch to account {account_info['name']} after {max_retries} attempts")
                    raise
                
                print("Retrying...")
                time.sleep(5)
                
                # Try to close any open menus before retry
                try:
                    self.driver.find_element(By.TAG_NAME, 'body').click()
                except:
                    pass

    def is_logged_out(self):
        """Check if we're on the login page"""
        try:
            # Look for login page elements
            login_elements = self.driver.find_elements(By.XPATH, "//input[@type='email' or @type='password']")
            if login_elements:
                print("Found login form - we're logged out")
                return True
            return False
        except Exception as e:
            print(f"Error checking login status: {str(e)}")
            return True  # Assume logged out if there's an error

    def get_current_account(self):
        """Get the name of the currently active account with retries and multiple selectors"""
        max_retries = 3
        selectors = [
            "//button[contains(@class, 'inline-flex') and .//img[@alt[contains(., 'Avatar')]]]//span[1]",
            "//button[@aria-haspopup='true']//span[contains(@class, 'text-sm')]",
            "//button[contains(@class, 'inline-flex')]//span[contains(@class, 'truncate')]"
        ]
        
        for attempt in range(max_retries):
            try:
                for selector in selectors:
                    try:
                        account_element = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, selector))
                        )
                        account_name = account_element.text.strip()
                        if account_name:
                            print(f"Current account: {account_name}")
                            return account_name
                    except:
                        continue
                
                # If we get here, none of the selectors worked
                raise Exception("Could not find account name with any selector")
                
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Failed to get current account name after {max_retries} attempts")
                    self.driver.save_screenshot("current_account_error.png")
                    raise
                print(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(5)

    def scrape_all_accounts(self):
        """Scrape data for all enabled accounts"""
        try:
            print("\nStarting scrape of all accounts...")
            
            # Get list of available accounts
            accounts = self.get_available_accounts()
            if not accounts:
                raise Exception("No accounts found to scrape")
            
            processed_accounts = []
            failed_accounts = []
            
            for account in accounts:
                try:
                    print(f"\n=== Processing account: {account['name']} ===")
                    
                    # Skip if we've already processed this account
                    if account['name'] in processed_accounts:
                        print(f"Already processed {account['name']}, skipping...")
                        continue
                    
                    # Try to switch to account with retries
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            if self.switch_to_account(account):
                                break
                        except Exception as e:
                            if attempt == max_retries - 1:
                                raise
                            print(f"Attempt {attempt + 1} failed, retrying...")
                            time.sleep(5)
                    
                    # Verify we switched successfully
                    current_account = self.get_current_account()
                    if current_account != account['name']:
                        raise Exception(f"Failed to switch to {account['name']}, current account is {current_account}")
                    
                    # Navigate to creator network and scrape data
                    print(f"Scraping data for {account['name']}...")
                    data = self.scrape_referral_data()
                    
                    if data:
                        processed_accounts.append(account['name'])
                        print(f"✅ Successfully processed {account['name']}")
                    else:
                        failed_accounts.append(account['name'])
                        print(f"❌ Failed to get data for {account['name']}")
                    
                except Exception as e:
                    print(f"❌ Error processing {account['name']}: {str(e)}")
                    failed_accounts.append(account['name'])
                    self.save_screenshot(f"account_error_{account['name'].lower().replace(' ', '_')}.png")
                    continue
            
            # Summary
            print("\n=== Scraping Summary ===")
            print(f"Successfully processed {len(processed_accounts)} accounts:")
            for acc in processed_accounts:
                print(f"✅ {acc}")
            
            if failed_accounts:
                print(f"\nFailed to process {len(failed_accounts)} accounts:")
                for acc in failed_accounts:
                    print(f"❌ {acc}")
            
            return processed_accounts, failed_accounts
            
        except Exception as e:
            print(f"Failed to scrape all accounts: {str(e)}")
            self.save_screenshot("scrape_all_accounts_error.png")
            raise

    def navigate_to_creator_network(self):
        """Navigate to the Creator Network page"""
        try:
            creator_network_url = "https://app.kit.com/creator-network"
            print(f"Navigating to Creator Network: {creator_network_url}")
            self.driver.get(creator_network_url)
            
            # Wait for navigation
            time.sleep(5)
            
            # Verify we reached the correct page
            current_url = self.driver.current_url
            print(f"Current URL after navigation: {current_url}")
            
            if "creator-network" not in current_url:
                print("⚠️ Navigation failed - not on Creator Network page")
                # Take screenshot for debugging
                self.driver.save_screenshot("navigation_failed.png")
                return False
            
            print("✅ Successfully navigated to Creator Network")
            return True
            
        except Exception as e:
            print(f"Failed to navigate to Creator Network: {str(e)}")
            self.driver.save_screenshot("navigation_error.png")
            raise

    def scrape_partner_avatars(self):
        """Scrape profile images from all allowed accounts"""
        try:
            print("\nStarting avatar collection...")
            
            # Create images directory if it doesn't exist
            image_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'avatars')
            os.makedirs(image_dir, exist_ok=True)
            print(f"Saving avatars to: {image_dir}")
            
            # Get all allowed accounts
            print("\nGetting available accounts...")
            accounts = self.get_available_accounts()
            avatars_collected = {}
            
            # Process each account
            for account in accounts:
                try:
                    print(f"\nProcessing account: {account['name']}")
                    if self.switch_to_account(account):
                        self.navigate_to_creator_network()
                        time.sleep(3)
                        
                        # Find all profile entries
                        profile_elements = self.driver.find_elements(
                            By.XPATH,
                            "//td[contains(@class, 'px-4 py-4')]//img[contains(@class, 'rounded-full')]/.."
                        )
                        
                        print(f"Found {len(profile_elements)} potential profiles")
                        
                        for profile in profile_elements:
                            try:
                                # Get the image and name
                                img = profile.find_element(By.XPATH, ".//img")
                                name_element = profile.find_element(By.XPATH, ".//div[contains(@class, 'inline text-sm')]")
                                
                                image_url = img.get_attribute('src')
                                creator_name = name_element.text.strip()
                                
                                if not image_url or not creator_name:
                                    continue
                                    
                                if creator_name not in avatars_collected:
                                    print(f"Processing avatar for: {creator_name}")
                                    
                                    # Download image
                                    response = requests.get(image_url)
                                    if response.status_code == 200:
                                        filename = f"{hashlib.md5(creator_name.encode()).hexdigest()}.png"
                                        filepath = os.path.join(image_dir, filename)
                                        
                                        with open(filepath, 'wb') as f:
                                            f.write(response.content)
                                        
                                        avatars_collected[creator_name] = filename
                                        print(f"✅ Saved avatar for {creator_name}")
                                else:
                                    print(f"Already have avatar for {creator_name}")
                                    
                            except Exception as e:
                                print(f"Error processing profile: {str(e)}")
                                continue
                            
                except Exception as e:
                    print(f"Error processing account {account['name']}: {str(e)}")
                    continue
            
            # Save mapping of names to image files
            mapping_file = os.path.join(image_dir, 'avatar_mapping.json')
            with open(mapping_file, 'w') as f:
                json.dump(avatars_collected, f, indent=2)
            
            print(f"\n✅ Collection complete!")
            print(f"Collected {len(avatars_collected)} unique avatars")
            print(f"Mapping saved to: {mapping_file}")
            
            return avatars_collected
            
        except Exception as e:
            print(f"Failed to scrape avatars: {str(e)}")
            self.driver.save_screenshot("avatar_scraping_error.png")
            raise

    def save_screenshot(self, name):
        """Save a screenshot with error context"""
        try:
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            filename = f"error_{name}_{timestamp}.png"
            self.driver.save_screenshot(filename)
            print(f"Screenshot saved as {filename}")
        except Exception as e:
            print(f"Failed to save screenshot: {str(e)}")

    def save_data(self, account_name, data):
        """Save scraped data to CSV"""
        try:
            # Create DataFrame from scraped data
            df = pd.DataFrame(data)
            
            # Add timestamp and account columns
            df['timestamp'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            df['account'] = account_name
            
            # Define CSV path
            csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'referral_data.csv')
            print(f"Using absolute path: {csv_path}")
            
            # Append to CSV (create if doesn't exist)
            df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)
            print(f"Data saved for account: {account_name}")
            
        except Exception as e:
            print(f"Error saving data for {account_name}: {str(e)}")
            print(f"Current working directory: {os.getcwd()}")