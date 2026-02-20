#!/usr/bin/env python3
"""
StockAnalysis OHLCV Collector - DEBUG VERSION
Enhanced diagnostics for download timeout issue

Date: February 15, 2026
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

# Import config
import stockanalysis_config as config

# Path setup
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(SCRIPT_DIR, 'temp')
os.makedirs(TEMP_DIR, exist_ok=True)

# Setup logging
LOG_FILE = os.path.join('logs', f'stockanalysis_debug_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more info
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PriceCollectorDebug:
    """Debug version with enhanced diagnostics"""

    def __init__(self):
        self.driver = None
        # Download directory
        self.download_dir = os.path.join(os.getcwd(), 'downloads')
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Output directory
        today = datetime.now().strftime('%Y-%m-%d')
        self.output_dir = os.path.join('output', today)
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info("=" * 80)
        logger.info("DIRECTORY DIAGNOSTICS")
        logger.info("=" * 80)
        logger.info(f"Script dir: {SCRIPT_DIR}")
        logger.info(f"Download dir: {self.download_dir}")
        logger.info(f"Output dir: {self.output_dir}")
        
        # Check permissions
        self._check_permissions()

    def _check_permissions(self):
        """Check directory permissions"""
        logger.info("\nChecking directory permissions...")
        
        # Check download directory
        test_file = os.path.join(self.download_dir, 'test_write.txt')
        try:
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            logger.info(f"✅ Download dir writable: {self.download_dir}")
        except Exception as e:
            logger.error(f"❌ Download dir NOT writable: {e}")
        
        # Check absolute path
        abs_download = os.path.abspath(self.download_dir)
        logger.info(f"Absolute download path: {abs_download}")

    def setup_driver(self):
        """Initialize Chrome driver with ENHANCED download settings"""
        logger.info("\n" + "=" * 80)
        logger.info("CHROME DRIVER SETUP")
        logger.info("=" * 80)
        
        chrome_options = Options()
        chrome_options.add_argument('--headless=new')  # NEW headless mode
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # ENHANCED download preferences
        download_dir_abs = os.path.abspath(self.download_dir)
        
        prefs = {
            "download.default_directory": download_dir_abs,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,  # Disable safe browsing (can block downloads)
            "safebrowsing.disable_download_protection": True,  # NEW
            "profile.default_content_setting_values.automatic_downloads": 1,  # NEW - allow automatic downloads
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Add command line switches for download
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        logger.info(f"Download directory (absolute): {download_dir_abs}")
        logger.info(f"Download prefs: {prefs}")
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Execute CDP commands to enable downloads in headless mode
        self.driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": download_dir_abs
        })
        
        logger.info("✅ Chrome driver ready with CDP download behavior set")

    def login(self):
        """Login to StockAnalysis.com"""
        logger.info("\n" + "=" * 80)
        logger.info("LOGIN")
        logger.info("=" * 80)
        
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
            logger.info("✅ Login successful")
            return True
            
        except Exception as e:
            logger.error(f"❌ Login failed: {e}")
            return False

    def download_csv_debug(self):
        """Download CSV with ENHANCED debugging"""
        logger.info("\n" + "=" * 80)
        logger.info("CSV DOWNLOAD (ENHANCED DEBUG)")
        logger.info("=" * 80)
        
        try:
            # Clear download directory
            old_files = glob.glob(os.path.join(self.download_dir, "*.csv"))
            logger.info(f"\nClearing {len(old_files)} old CSV file(s)...")
            for f in old_files:
                os.remove(f)
                logger.info(f"   Removed: {os.path.basename(f)}")
            
            # Navigate to watchlist
            watchlist_url = "https://stockanalysis.com/watchlist/"
            logger.info(f"\nNavigating to: {watchlist_url}")
            self.driver.get(watchlist_url)
            time.sleep(5)
            
            # Click Daily_Price tab
            logger.info("\nLooking for Daily_Price tab...")
            tab_element = self.driver.find_element(
                By.XPATH,
                "//button[contains(text(), 'Daily_Price')]"
            )
            logger.info("   Found Daily_Price tab")
            tab_element.click()
            logger.info("   Clicked Daily_Price tab")
            time.sleep(3)
            
            # Find Options button
            logger.info("\nFinding Options button...")
            chart_view = self.driver.find_element(
                By.XPATH,
                "//button[contains(text(), 'Chart View')]"
            )
            logger.info("   Found Chart View anchor")
            
            parent = chart_view.find_element(By.XPATH, "..")
            options_button = parent.find_element(
                By.XPATH,
                ".//button[contains(., 'Options')]"
            )
            logger.info("   Found Options button")
            
            # Click Options
            logger.info("\nClicking Options dropdown...")
            options_button.click()
            logger.info("   Options dropdown opened")
            time.sleep(1)
            
            # Take screenshot BEFORE clicking download
            screenshot_path = os.path.join(self.download_dir, "before_download.png")
            self.driver.save_screenshot(screenshot_path)
            logger.info(f"   📸 Screenshot saved: {screenshot_path}")
            
            # Click Download to CSV
            logger.info("\nClicking 'Download to CSV'...")
            download_option = self.driver.find_element(
                By.XPATH,
                "//div[contains(text(), 'Download to CSV')]"
            )
            logger.info("   Found Download to CSV option")
            
            # Log element details
            logger.debug(f"   Element tag: {download_option.tag_name}")
            logger.debug(f"   Element text: {download_option.text}")
            logger.debug(f"   Element visible: {download_option.is_displayed()}")
            
            download_option.click()
            logger.info("   ✅ Clicked Download to CSV")
            
            # Take screenshot AFTER clicking download
            screenshot_path2 = os.path.join(self.download_dir, "after_download_click.png")
            self.driver.save_screenshot(screenshot_path2)
            logger.info(f"   📸 Screenshot saved: {screenshot_path2}")
            
            # Wait for download with ENHANCED monitoring
            logger.info("\n" + "=" * 80)
            logger.info("DOWNLOAD MONITORING (Enhanced)")
            logger.info("=" * 80)
            logger.info(f"Watching directory: {self.download_dir}")
            
            max_wait = 60  # Increased to 60 seconds
            for i in range(max_wait):
                # Check for CSV files
                csv_files = glob.glob(os.path.join(self.download_dir, "*.csv"))
                
                # Check for in-progress downloads
                crdownload_files = glob.glob(os.path.join(self.download_dir, "*.crdownload"))
                
                # Check for all files
                all_files = glob.glob(os.path.join(self.download_dir, "*"))
                
                if i % 5 == 0:  # Log every 5 seconds
                    logger.info(f"   [{i:2d}s] CSV files: {len(csv_files)}, In-progress: {len(crdownload_files)}, All files: {len(all_files)}")
                    if all_files:
                        for f in all_files:
                            logger.debug(f"      - {os.path.basename(f)} ({os.path.getsize(f)} bytes)")
                
                # Check if download complete
                if csv_files and not crdownload_files:
                    logger.info(f"\n✅ Download complete! ({i+1} seconds)")
                    logger.info(f"   File: {os.path.basename(csv_files[0])}")
                    logger.info(f"   Size: {os.path.getsize(csv_files[0])} bytes")
                    return csv_files[0]
                
                time.sleep(1)
            
            # Timeout - show final state
            logger.error("\n❌ Download timeout after 60 seconds")
            logger.error("\nFinal directory state:")
            all_files = glob.glob(os.path.join(self.download_dir, "*"))
            if all_files:
                for f in all_files:
                    logger.error(f"   - {os.path.basename(f)} ({os.path.getsize(f)} bytes)")
            else:
                logger.error("   (directory is empty)")
            
            # Check browser console for errors
            logger.info("\nChecking browser console logs...")
            try:
                logs = self.driver.get_log('browser')
                if logs:
                    logger.error("Browser console errors:")
                    for entry in logs[-10:]:  # Last 10 entries
                        logger.error(f"   {entry}")
            except:
                logger.info("   Could not retrieve console logs")
            
            return None
            
        except Exception as e:
            logger.error(f"\n❌ Download failed: {e}")
            import traceback
            traceback.print_exc()
            
            # Take error screenshot
            screenshot_path = os.path.join(self.download_dir, "error_screenshot.png")
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.info(f"   📸 Error screenshot: {screenshot_path}")
            except:
                pass
            
            return None

    def run(self):
        """Main execution with debug mode"""
        logger.info("\n" + "=" * 80)
        logger.info("STOCKANALYSIS OHLCV COLLECTOR - DEBUG MODE")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        
        try:
            self.setup_driver()
            
            if not self.login():
                return None
            
            # Download CSV (debug version)
            downloaded_csv = self.download_csv_debug()
            
            if downloaded_csv:
                logger.info("\n" + "=" * 80)
                logger.info("✅ SUCCESS!")
                logger.info("=" * 80)
                logger.info(f"Downloaded file: {downloaded_csv}")
                return downloaded_csv
            else:
                logger.error("\n" + "=" * 80)
                logger.error("❌ DOWNLOAD FAILED")
                logger.error("=" * 80)
                return None
                
        except Exception as e:
            logger.error(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return None
        
        finally:
            if self.driver:
                logger.info("\nClosing browser...")
                time.sleep(2)
                self.driver.quit()


def main():
    """Entry point"""
    collector = PriceCollectorDebug()
    csv_file = collector.run()
    
    if csv_file:
        temp_file = os.path.join(TEMP_DIR, 'latest_prices.txt')
        with open(temp_file, 'w') as f:
            f.write(csv_file)
        
        logger.info(f"\n✅ Path saved to: {temp_file}")
        sys.exit(0)
    else:
        logger.error("\n❌ Collection failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
