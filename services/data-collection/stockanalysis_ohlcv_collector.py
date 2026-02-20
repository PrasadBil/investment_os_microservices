
#!/usr/bin/env python3
"""
Price Collector - CSV DOWNLOAD with SPATIAL TRIANGULATION
Uses Chart View as anchor point to find Options dropdown
Runtime: ~15 seconds

Migration: Phase 2 (Feb 2026)
- Changed: /tmp/latest_prices.txt -> SCRIPT_DIR/temp/latest_prices.txt
- Changed: import config -> import stockanalysis_config as config
- Original: /opt/selenium_automation/price_collector.py
"""

import os
import sys
import time
import glob
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import logging

# Import config (Phase 2: renamed to avoid collision with common.config)
import stockanalysis_config as config

# === Path setup (Phase 2 Migration) ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(SCRIPT_DIR, 'temp')
os.makedirs(TEMP_DIR, exist_ok=True)

# Setup logging
LOG_FILE = os.path.join('logs', f'price_collection_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PriceCollector:
    """CSV download with spatial triangulation"""

    def __init__(self):
        self.driver = None
        # Download directory (MUST be absolute for Chrome headless)
        self.download_dir = os.path.abspath(os.path.join(os.getcwd(), 'downloads'))
        os.makedirs(self.download_dir, exist_ok=True)

        # Output directory
        today = datetime.now().strftime('%Y-%m-%d')
        self.output_dir = os.path.join('output', today)
        os.makedirs(self.output_dir, exist_ok=True)

    def setup_driver(self):
        """Initialize Chrome driver with download settings"""
        logger.info("Setting up Chrome driver...")

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')

        # Set download directory (absolute path required for headless Chrome)
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "safebrowsing.disable_download_protection": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # CDP command: Enable downloads in headless Chrome
        # Modern headless Chrome requires explicit download permission via DevTools Protocol
        self.driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": self.download_dir
        })

        logger.info("Chrome driver ready")
        logger.info(f"Download directory: {self.download_dir}")

    def login(self):
        """Login to StockAnalysis.com"""
        logger.info("Logging in to StockAnalysis.com...")

        # Fail fast: check credentials are present
        if not config.EMAIL or not config.PASSWORD:
            logger.error("STOCKANALYSIS_EMAIL or STOCKANALYSIS_PASSWORD not set in .env!")
            logger.error("Cannot login with empty credentials.")
            return False

        try:
            self.driver.get(config.LOGIN_URL)
            time.sleep(3)

            email_input = self.driver.find_element(By.ID, "email")
            email_input.clear()
            email_input.send_keys(config.EMAIL)

            password_input = self.driver.find_element(By.ID, "password")
            password_input.clear()
            password_input.send_keys(config.PASSWORD)

            login_button = self.driver.find_element(
                By.XPATH,
                "//button[contains(text(), 'Log in') or contains(text(), 'Log In')]"
            )
            login_button.click()

            time.sleep(5)

            # Verify login actually succeeded (don't trust blind wait)
            current_url = self.driver.current_url
            if '/login' in current_url:
                logger.error(f"Login failed - still on login page: {current_url}")
                # Take screenshot for debugging
                screenshot_path = os.path.join(self.download_dir, "login_failed.png")
                self.driver.save_screenshot(screenshot_path)
                logger.error(f"Screenshot saved: {screenshot_path}")
                return False

            logger.info("Login successful")
            logger.info(f"Post-login URL: {current_url}")
            return True

        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    def download_csv(self):
        """Download CSV using spatial triangulation"""
        logger.info("=" * 70)
        logger.info("DOWNLOADING CSV (SPATIAL TRIANGULATION)")
        logger.info("=" * 70)

        try:
            # Clear download directory
            for f in glob.glob(os.path.join(self.download_dir, "*.csv")):
                os.remove(f)
                logger.info(f"   Cleared old file: {os.path.basename(f)}")

            # Navigate to watchlist
            watchlist_url = "https://stockanalysis.com/watchlist/"
            logger.info(f"Going to: {watchlist_url}")
            self.driver.get(watchlist_url)
            time.sleep(5)

            # Click Daily_Price tab
            logger.info("Looking for 'Daily_Price' tab...")
            tab_selectors = [
                "//button[contains(text(), 'Daily_Price')]",
                "//button[text()='Daily_Price']",
            ]

            for selector in tab_selectors:
                try:
                    tab_element = self.driver.find_element(By.XPATH, selector)
                    logger.info("   Found Daily_Price tab")
                    try:
                        tab_element.click()
                    except:
                        self.driver.execute_script("arguments[0].click();", tab_element)
                    logger.info("   Clicked Daily_Price tab")
                    break
                except:
                    continue

            # Wait for data to load
            time.sleep(3)

            # SPATIAL TRIANGULATION: Find Options button
            logger.info("Using spatial triangulation to find Options...")
            logger.info("   Hint: Options is next to 'Chart View' button")

            options_button = None

            # Strategy 1: Find by button role and text
            try:
                options_button = self.driver.find_element(
                    By.XPATH,
                    "//button[@role='button' and contains(., 'Options')]"
                )
                logger.info("   Found Options (Strategy 1: role + text)")
            except:
                pass

            # Strategy 2: Find Chart View, then look nearby
            if not options_button:
                try:
                    chart_view = self.driver.find_element(
                        By.XPATH,
                        "//button[contains(text(), 'Chart View')]"
                    )
                    logger.info("   Found Chart View anchor")

                    # Options should be a sibling or nearby
                    parent = chart_view.find_element(By.XPATH, "..")
                    options_button = parent.find_element(
                        By.XPATH,
                        ".//button[contains(., 'Options')]"
                    )
                    logger.info("   Found Options (Strategy 2: near Chart View)")
                except:
                    pass

            # Strategy 3: Find any button with "Options" text
            if not options_button:
                try:
                    options_button = self.driver.find_element(
                        By.XPATH,
                        "//button[contains(text(), 'Options')]"
                    )
                    logger.info("   Found Options (Strategy 3: simple text match)")
                except:
                    pass

            # Strategy 4: Find by common button patterns
            if not options_button:
                try:
                    options_button = self.driver.find_element(
                        By.XPATH,
                        "//button[contains(@class, 'dropdown') or contains(., '\u25bc')]"
                    )
                    logger.info("   Found Options (Strategy 4: dropdown pattern)")
                except:
                    pass

            if not options_button:
                logger.error("   Failed to find Options button with all strategies")
                screenshot_path = os.path.join(self.download_dir, "debug_screenshot.png")
                self.driver.save_screenshot(screenshot_path)
                logger.info(f"   Saved screenshot: {screenshot_path}")
                return None

            # Click Options button
            logger.info("   Clicking Options dropdown...")
            try:
                options_button.click()
            except:
                self.driver.execute_script("arguments[0].click();", options_button)

            logger.info("   Options dropdown opened")
            time.sleep(1)

            # Click "Download to CSV"
            logger.info("Looking for 'Download to CSV' option...")

            download_selectors = [
                "//div[contains(text(), 'Download to CSV')]",
                "//span[contains(text(), 'Download to CSV')]",
                "//button[contains(text(), 'Download to CSV')]",
                "//*[contains(text(), 'Download') and contains(text(), 'CSV')]",
            ]

            download_clicked = False
            for selector in download_selectors:
                try:
                    download_option = self.driver.find_element(By.XPATH, selector)
                    logger.info(f"   Found Download to CSV")
                    download_option.click()
                    logger.info("   Clicked Download to CSV")
                    download_clicked = True
                    break
                except:
                    continue

            if not download_clicked:
                logger.error("   Failed to find Download to CSV option")
                return None

            # Wait for download to complete (extended timeout for headless mode)
            logger.info("   Waiting for download to complete...")
            max_wait = 60
            for i in range(max_wait):
                all_files = glob.glob(os.path.join(self.download_dir, "*"))
                csv_files = glob.glob(os.path.join(self.download_dir, "*.csv"))
                crdownload_files = [f for f in all_files if f.endswith('.crdownload')]

                if csv_files and not crdownload_files:
                    csv_size = os.path.getsize(csv_files[0])
                    logger.info(f"   Download complete! ({i+1}s, {csv_size} bytes)")
                    return csv_files[0]

                # Progress logging every 10 seconds
                if (i + 1) % 10 == 0:
                    logger.info(f"   Still waiting... ({i+1}s, files in dir: {len(all_files)}, "
                                f"csv: {len(csv_files)}, partial: {len(crdownload_files)})")

                time.sleep(1)

            # Timeout - capture debug info
            logger.error(f"   Download timeout after {max_wait}s")
            all_files = glob.glob(os.path.join(self.download_dir, "*"))
            logger.error(f"   Files in download dir: {all_files}")
            screenshot_path = os.path.join(self.download_dir, "download_timeout.png")
            self.driver.save_screenshot(screenshot_path)
            logger.error(f"   Screenshot saved: {screenshot_path}")
            return None

        except Exception as e:
            logger.error(f"Download failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def process_csv(self, downloaded_csv):
        """Process downloaded CSV"""
        logger.info("")
        logger.info("=" * 70)
        logger.info("PROCESSING DOWNLOADED CSV")
        logger.info("=" * 70)
        logger.info(f"Input file: {os.path.basename(downloaded_csv)}")

        try:
            # Read CSV
            df = pd.read_csv(downloaded_csv)

            logger.info(f"Original rows: {len(df)}")
            logger.info(f"Original columns: {list(df.columns)}")

            # Rename columns
            df.columns = ['symbol', 'price']

            # Remove "COSE:" prefix
            df['symbol'] = df['symbol'].str.replace('COSE:', '', regex=False)

            # Fix ATLN edge case (StockAnalysis.com data issue)
            # Their CSV shows "COSE:ATLN" instead of "COSE:ATLN.N0000"
            df['symbol'] = df['symbol'].replace('ATLN', 'ATLN.N0000')

            logger.info(f"   Fixed ATLN suffix (ATLN -> ATLN.N0000)")

            # Add metadata
            df['date'] = datetime.now().strftime('%Y-%m-%d')
            df['time'] = datetime.now().strftime('%H:%M:%S')
            df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Reorder
            df = df[['symbol', 'price', 'date', 'time', 'timestamp']]

            # Save
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"prices_{timestamp}.csv"
            filepath = os.path.join(self.output_dir, filename)

            df.to_csv(filepath, index=False)

            logger.info(f"Processed and saved: {filename}")
            logger.info(f"Final rows: {len(df)}")
            logger.info("")
            logger.info("Sample (first 10):")
            logger.info(df.head(10).to_string(index=False))
            logger.info("")
            logger.info("Last 5:")
            logger.info(df.tail(5).to_string(index=False))

            return filepath

        except Exception as e:
            logger.error(f"Processing failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self):
        """Main execution"""
        logger.info("=" * 70)
        logger.info("CSV DOWNLOAD (SPATIAL TRIANGULATION)")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        try:
            self.setup_driver()

            if not self.login():
                return None

            # Download CSV
            downloaded_csv = self.download_csv()
            if not downloaded_csv:
                logger.error("CSV download failed")
                return None

            # Process CSV
            output_csv = self.process_csv(downloaded_csv)

            if output_csv:
                logger.info("")
                logger.info("=" * 70)
                logger.info("COLLECTION COMPLETE!")
                logger.info("=" * 70)
                logger.info(f"File: {output_csv}")
                logger.info("=" * 70)
                return output_csv
            else:
                return None

        except Exception as e:
            logger.error(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return None

        finally:
            if self.driver:
                logger.info("\nClosing browser...")
                time.sleep(1)
                self.driver.quit()


def main():
    """Entry point"""
    collector = PriceCollector()
    csv_file = collector.run()

    if csv_file:
        # Phase 2: temp/ instead of /tmp/
        temp_file = os.path.join(TEMP_DIR, 'latest_prices.txt')
        with open(temp_file, 'w') as f:
            f.write(csv_file)
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
