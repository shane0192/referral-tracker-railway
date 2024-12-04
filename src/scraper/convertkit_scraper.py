from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import logging
import time
import pickle
import os
import sys
import json
import csv
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.data.db_manager import DatabaseManager
import pandas as pd
from ..utils.config import CONVERTKIT_EMAIL, CONVERTKIT_PASSWORD
import requests
import hashlib

# Add this constant
ALLOWED_ACCOUNTS = [
    "Adam Graham",
    "ATH Media LLC",
    "Chris Donnelly",
    "Eric Partaker",
    "Good Good Good",
    "Life's A Game with Amanda Goetz",
    "Micro-Agency Launchpad",
    "Nathan Barry",
    "The Perfect Loaf"
]

class ConvertKitScraper:
    def __init__(self, chrome_profile_path=None):
        """Initialize the scraper with login credentials"""
        self.email = CONVERTKIT_EMAIL
        self.password = CONVERTKIT_PASSWORD
        
        # Create a dedicated profile directory in the project folder
        self.profile_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'automation_chrome_profile')
        if not os.path.exists(self.profile_dir):
            os.makedirs(self.profile_dir)
            print("\n⚠️ First time setup: You'll need to log in manually once and verify 2FA")
            print("After this, the session should persist for about a month")
        
        print(f"Using Chrome profile at: {self.profile_dir}")
        
        # Setup Chrome options
        chrome_options = Options()
        chrome_options.add_argument(f'user-data-dir={self.profile_dir}')
        chrome_options.add_argument('--profile-directory=Default')
        
        try:
            print("\nInitializing Chrome driver...")
            self.driver = webdriver.Chrome(options=chrome_options)
            print("Chrome driver initialized successfully")
            
        except Exception as e:
            print(f"Failed to initialize Chrome driver: {str(e)}")
            raise

        self.current_account = None  # Add this to track current account

    def setup_driver(self):
        """Initialize the Chrome WebDriver"""
        try:
            options = webdriver.ChromeOptions()
            
            # Set up a persistent profile directory
            user_data_dir = os.path.join(os.getcwd(), 'chrome_profile')
            options.add_argument(f'user-data-dir={user_data_dir}')
            
            # Other options
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--window-size=1920,1080')
            
            print(f"Using Chrome profile at: {user_data_dir}")
            
            # Create service
            service = webdriver.ChromeService()
            
            # Initialize driver with service and options
            self.driver = webdriver.Chrome(service=service, options=options)
            
        except Exception as e:
            print(f"Failed to setup driver: {str(e)}")
            raise

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

    def scrape_referral_data(self):
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
            
            # Save to CSV
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
                        
                        # Check if this account is in our allowed list
                        if clean_name in ALLOWED_ACCOUNTS:
                            accounts.append({
                                'name': clean_name,
                                'email': element.get_attribute('data-account-email'),
                                'element': element
                            })
                            print(f"Added allowed account: {clean_name}")
                        else:
                            print(f"Skipping non-allowed account: {clean_name}")
                except Exception as e:
                    print(f"Error processing element: {str(e)}")
                    continue
            
            if not accounts:
                print("\n🚨 No allowed accounts found in dropdown!")
                print("Taking screenshot for debugging...")
                self.driver.save_screenshot("no_accounts_found.png")
                raise Exception("No allowed accounts found in dropdown")
            
            print(f"\n✅ Successfully found {len(accounts)} allowed accounts: {', '.join(acc['name'] for acc in accounts)}")
            return accounts
            
        except Exception as e:
            print(f"\n❌ Error getting available accounts: {str(e)}")
            self.driver.save_screenshot("get_accounts_error.png")
            raise

    def switch_to_account(self, account_info):
        """Switch to a different account"""
        try:
            print(f"Attempting to switch to account: {account_info['name']}")
            
            # Use the same selector that works in get_current_account()
            menu_selector = "//button[contains(@class, 'inline-flex') and .//img[@alt[contains(., 'Avatar')]]]"
            menu_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, menu_selector))
            )
            
            print("Found account menu button, clicking...")
            menu_button.click()
            time.sleep(2)
            
            # Find and click account link
            account_link = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR, 
                    f"a[role='menuitem'][data-account-email='{account_info['email']}']"
                ))
            )
            
            print(f"Found account link, clicking...")
            account_link.click()
            time.sleep(5)
            
            self.current_account = account_info['name']
            print(f"Switched to account: {account_info['name']}")
            
            return True
                
        except Exception as e:
            print(f"Failed to switch to account {account_info['name']}: {str(e)}")
            self.driver.save_screenshot(f"account_switch_error_{account_info['name'].lower().replace(' ', '_')}.png")
            raise

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
        """Get the name of the currently active account"""
        try:
            # Look for the account button in the top right
            account_button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'inline-flex') and .//img[@alt[contains(., 'Avatar')]]]//span[1]"))
            )
            # Get the first line of text (account name)
            account_name = account_button.text.strip()
            print(f"Current account: {account_name}")
            return account_name
        
        except Exception as e:
            print(f"Failed to get current account name: {str(e)}")
            self.driver.save_screenshot("current_account_error.png")
            raise

    def scrape_all_accounts(self, account_list):
        """Scrape data for multiple accounts"""
        for account in account_list:
            if self.switch_account(account):
                print(f"Scraping data for {account}...")
                self.scrape_referral_data()
            else:
                print(f"Skipping {account} due to switch error")

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