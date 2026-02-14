
#!/usr/bin/env python3
"""
CSE Price Collector - STEP 1: Download CSV
Downloads raw CSV from CSE Trade Summary
Runtime: ~15 seconds

Migration: Phase 2 (Feb 2026)
- Changed: /tmp/latest_cse_raw.txt -> SCRIPT_DIR/temp/latest_cse_raw.txt
- Original: /opt/selenium_automation/cse_collector.py
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
import logging

# === Path setup (Phase 2 Migration) ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(SCRIPT_DIR, 'temp')
os.makedirs(TEMP_DIR, exist_ok=True)

# Setup logging
LOG_FILE = os.path.join('logs', f'cse_collector_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
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


class CSECollector:
    """Download CSV from CSE Trade Summary"""

    def __init__(self):
        self.driver = None
        self.download_dir = os.path.join(os.getcwd(), 'downloads')
        os.makedirs(self.download_dir, exist_ok=True)
        self.cse_url = "https://www.cse.lk/equity/trade-summary"

    def setup_driver(self):
        """Initialize Chrome driver"""
        logger.info("Setting up Chrome driver...")

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')

        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        logger.info("Chrome driver ready")

    def download_csv(self):
        """Download CSV from CSE using Prasad's simple logic"""
        logger.info("=" * 70)
        logger.info("DOWNLOADING CSE TRADE SUMMARY CSV")
        logger.info("=" * 70)

        try:
            # Clear old files
            for f in glob.glob(os.path.join(self.download_dir, "*.csv")):
                os.remove(f)

            # Navigate
            logger.info(f"Navigating to: {self.cse_url}")
            self.driver.get(self.cse_url)
            time.sleep(5)
            logger.info("Page loaded")

            # Take screenshot for debugging
            screenshot_path = os.path.join(self.download_dir, "step1_page_loaded.png")
            self.driver.save_screenshot(screenshot_path)
            logger.info(f"Screenshot: {screenshot_path}")

            # STEP 1: Select "All" entries
            # PRASAD'S SIMPLE LOGIC: Find "25" between "Show" and "Entries", click it, select last option
            logger.info("")
            logger.info("STEP 1: Selecting 'All' entries...")
            logger.info("   Using Prasad's simple logic: Click '25' -> Select last option")

            all_selected = False

            # Strategy: Find "25" text, click it, select last option from dropdown
            try:
                # Find the "25" text (it's the clickable button showing current selection)
                logger.info("   Looking for '25' button...")

                # Try finding span/button/div containing "25"
                twenty_five = None

                # Look for "25" near "Show" text
                try:
                    show_elem = self.driver.find_element(
                        By.XPATH,
                        "//*[contains(text(), 'Show')]"
                    )
                    logger.info("   Found 'Show' text")

                    # Look for "25" nearby
                    parent = show_elem.find_element(By.XPATH, "..")
                    twenty_five = parent.find_element(
                        By.XPATH,
                        ".//*[contains(text(), '25')]"
                    )
                    logger.info("   Found '25' button near 'Show'")

                except:
                    # Alternative: just search for any clickable element with "25"
                    twenty_five = self.driver.find_element(
                        By.XPATH,
                        "//button[contains(text(), '25')] | //span[contains(text(), '25')] | //a[contains(text(), '25')]"
                    )
                    logger.info("   Found '25' element")

                # Click on "25" to open dropdown
                logger.info("   Clicking '25' to open dropdown...")
                try:
                    twenty_five.click()
                except:
                    # Try JavaScript click
                    self.driver.execute_script("arguments[0].click();", twenty_five)

                logger.info("   Clicked '25', dropdown should be open")
                time.sleep(1)

                # Now find the dropdown menu and select last option (All)
                logger.info("   Looking for 'All' option in dropdown...")

                # Try to find "All" option
                all_option = None

                # Strategy A: Look for li/a/button with "All" text
                try:
                    all_option = self.driver.find_element(
                        By.XPATH,
                        "//li[text()='All'] | //a[text()='All'] | //button[text()='All'] | //*[text()='All']"
                    )
                    logger.info("   Found 'All' option")
                except:
                    pass

                # Strategy B: Find all dropdown options and click last one
                if not all_option:
                    try:
                        options = self.driver.find_elements(
                            By.XPATH,
                            "//li[@role='option'] | //a[@role='option'] | //div[@role='option']"
                        )
                        if options:
                            all_option = options[-1]  # Last option
                            logger.info(f"   Found dropdown with {len(options)} options, selecting last one")
                    except:
                        pass

                if all_option:
                    # Click "All" option
                    logger.info("   Clicking 'All' option...")
                    try:
                        all_option.click()
                    except:
                        self.driver.execute_script("arguments[0].click();", all_option)

                    logger.info("   Selected 'All' entries!")
                    all_selected = True
                    time.sleep(3)  # Wait for table to reload with all entries
                else:
                    logger.warning("   Could not find 'All' option in dropdown")

            except Exception as e:
                logger.info(f"   Strategy failed: {e}")
                logger.warning("   Could not select 'All' - using default view")

            if not all_selected:
                logger.warning("   Could not select 'All' - using default view (20-25 stocks)")

            # Screenshot after selection
            screenshot_path = os.path.join(self.download_dir, "step2_all_selected.png")
            self.driver.save_screenshot(screenshot_path)
            logger.info(f"Screenshot: {screenshot_path}")

            # STEP 2: Find Download button
            # Strategy: Blue button ABOVE date/time stamp (THIS IS WORKING!)
            logger.info("")
            logger.info("STEP 2: Finding Download button...")
            logger.info("   Using spatial triangulation (date/time anchor)...")

            download_button = None

            # Strategy A: Find date/time, then look for button above it (THIS WORKS!)
            try:
                logger.info("   Strategy A: Finding date/time as anchor...")

                # Look for date/time pattern
                datetime_elem = self.driver.find_element(
                    By.XPATH,
                    "//*[contains(text(), '202') or contains(text(), ':')]"
                )
                logger.info("   Found date/time anchor")

                # Get parent container
                container = datetime_elem.find_element(By.XPATH, "../..")

                # Find button in same container
                download_button = container.find_element(
                    By.XPATH,
                    ".//button[contains(., 'Download') or contains(@class, 'btn')]"
                )
                logger.info("   Found Download button (near date/time)")

            except Exception as e:
                logger.info(f"   Strategy A failed: {e}")

            # Strategy B: Any button containing "Download"
            if not download_button:
                try:
                    download_button = self.driver.find_element(
                        By.XPATH,
                        "//button[contains(text(), 'Download')] | //a[contains(text(), 'Download')]"
                    )
                    logger.info("   Found Download button")
                except Exception as e:
                    logger.info(f"   Strategy B failed: {e}")

            if not download_button:
                logger.error("   Could not find Download button")
                screenshot_path = os.path.join(self.download_dir, "error_no_download_button.png")
                self.driver.save_screenshot(screenshot_path)
                return None

            # Click Download button
            logger.info("   Clicking Download button...")
            try:
                download_button.click()
            except:
                self.driver.execute_script("arguments[0].click();", download_button)

            logger.info("   Clicked Download button")
            time.sleep(2)

            # Screenshot after clicking
            screenshot_path = os.path.join(self.download_dir, "step3_clicked_download.png")
            self.driver.save_screenshot(screenshot_path)
            logger.info(f"Screenshot: {screenshot_path}")

            # STEP 3: Find and click CSV option (THIS IS WORKING!)
            logger.info("")
            logger.info("STEP 3: Finding CSV option...")

            csv_option = None

            # Strategy A: Button with CSV (THIS WORKS!)
            try:
                logger.info("   Strategy A: Looking for dropdown menu...")
                csv_option = self.driver.find_element(
                    By.XPATH,
                    "//a[contains(text(), 'CSV')] | //li[contains(text(), 'CSV')] | //div[contains(text(), 'CSV')]"
                )
                logger.info("   Found CSV option in dropdown")
            except Exception as e:
                logger.info(f"   Strategy A failed: {e}")

            # Strategy B: CSV button
            if not csv_option:
                try:
                    logger.info("   Strategy B: Looking for CSV button...")
                    csv_option = self.driver.find_element(
                        By.XPATH,
                        "//button[contains(text(), 'CSV')]"
                    )
                    logger.info("   Found CSV button")
                except Exception as e:
                    logger.info(f"   Strategy B failed: {e}")

            if not csv_option:
                logger.error("   Could not find CSV option")
                return None

            # Click CSV option
            logger.info("   Clicking CSV option...")
            try:
                csv_option.click()
            except:
                self.driver.execute_script("arguments[0].click();", csv_option)

            logger.info("   Clicked CSV option")

            # Wait for download
            logger.info("")
            logger.info("   Waiting for download to complete...")

            for i in range(30):
                csvs = glob.glob(os.path.join(self.download_dir, "*.csv"))
                temps = glob.glob(os.path.join(self.download_dir, "*.crdownload"))

                if csvs and not temps:
                    csv_file = csvs[0]
                    file_size = os.path.getsize(csv_file)
                    logger.info(f"   Downloaded: {os.path.basename(csv_file)} ({file_size:,} bytes)")
                    return csv_file

                time.sleep(1)

            logger.error("   Download timeout (30 seconds)")
            return None

        except Exception as e:
            logger.error(f"Download failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self):
        """Main execution"""
        logger.info("=" * 70)
        logger.info("CSE COLLECTOR - STEP 1")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        try:
            self.setup_driver()
            csv_file = self.download_csv()

            if csv_file:
                # Save path for processor (Phase 2: temp/ instead of /tmp/)
                temp_file = os.path.join(TEMP_DIR, 'latest_cse_raw.txt')
                with open(temp_file, 'w') as f:
                    f.write(csv_file)

                logger.info("")
                logger.info("=" * 70)
                logger.info("COLLECTION COMPLETE!")
                logger.info("=" * 70)
                logger.info(f"File: {csv_file}")
                return csv_file
            else:
                logger.error("Collection failed")
                return None

        except Exception as e:
            logger.error(f"Error: {e}")
            return None

        finally:
            if self.driver:
                self.driver.quit()


def main():
    collector = CSECollector()
    result = collector.run()
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
