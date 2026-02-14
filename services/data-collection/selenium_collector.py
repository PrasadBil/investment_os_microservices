#!/usr/bin/env python3
"""
StockAnalysis.com Data Collector - 13 VIEWS VERSION (NO PAGINATION!)
Simplified version: 13 views covering all 235 parameters, one page per view

Migration: Phase 2 (Feb 2026)
- Changed: import config -> import stockanalysis_config as config
- Original: /opt/selenium_automation/selenium_collector.py
"""

import os
import sys
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import logging

# Import config (Phase 2: renamed to avoid collision with common.config)
import stockanalysis_config as config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockAnalysisCollector:
    def __init__(self):
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Initialize Chrome driver"""
        logger.info("Setting up Chrome driver...")

        chrome_options = Options()
        # Headless mode for VPS
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        # Anti-detection options
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # DISABLE PASSWORD SAVE POPUP
        prefs = {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # Initialize driver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        logger.info("Chrome driver ready")

    def login(self):
        """Login to StockAnalysis.com"""
        logger.info("=" * 50)
        logger.info("STEP 1: Logging in to StockAnalysis.com")
        logger.info("=" * 50)

        try:
            self.driver.get(config.LOGIN_URL)
            time.sleep(3)

            # Fill email
            logger.info("Entering email...")
            email_input = self.driver.find_element(By.ID, "email")
            email_input.clear()
            email_input.send_keys(config.EMAIL)

            # Fill password
            logger.info("Entering password...")
            password_input = self.driver.find_element(By.ID, "password")
            password_input.clear()
            password_input.send_keys(config.PASSWORD)

            # Click login button
            logger.info("Clicking login button...")
            login_button = self.driver.find_element(
                By.XPATH,
                "//button[contains(text(), 'Log in') or contains(text(), 'Log In')]"
            )
            login_button.click()

            # Wait for login to complete
            time.sleep(5)

            logger.info("Login successful")
            return True

        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    def set_country_to_sri_lanka(self):
        """Check if country is Sri Lanka, if not, change it"""
        logger.info("Checking Exchange Country...")

        try:
            time.sleep(3)

            # Find country dropdown/button
            country_selectors = [
                "//button[contains(@class, 'controls-btn') and contains(., 'United States')]",
                "//button[contains(., 'US') or contains(., 'United States')]",
                "//*[contains(text(), 'Exchange Country')]/following-sibling::*/button",
                "//button[contains(@aria-label, 'country') or contains(@id, 'country')]",
            ]

            # Check current country
            country_button = None
            for selector in country_selectors:
                try:
                    country_button = self.driver.find_element(By.XPATH, selector)
                    button_text = country_button.text
                    logger.info(f"   Found country button: {button_text}")

                    # If already Sri Lanka, we're good!
                    if "Sri Lanka" in button_text or "LK" in button_text:
                        logger.info("   Already set to Sri Lanka")
                        return True

                    # If it's USA or other country, we need to change it
                    logger.info("   Country is not Sri Lanka, changing it...")
                    break
                except:
                    continue

            if country_button is None:
                logger.warning("Could not find country button, assuming it's already correct")
                return True

            # Click country dropdown
            country_button.click()
            time.sleep(2)

            # Find and click Sri Lanka option
            sri_lanka_selectors = [
                "//button[contains(text(), 'Sri Lanka')]",
                "//*[contains(text(), 'Sri Lanka')]",
                "//li[contains(text(), 'Sri Lanka')]",
                "//option[contains(text(), 'Sri Lanka')]",
            ]

            for selector in sri_lanka_selectors:
                try:
                    sri_lanka_option = self.driver.find_element(By.XPATH, selector)
                    sri_lanka_option.click()
                    logger.info("   Changed country to Sri Lanka")
                    time.sleep(3)
                    return True
                except:
                    continue

            logger.error("   Could not find Sri Lanka option")
            return False

        except Exception as e:
            logger.error(f"   Failed to set country: {e}")
            return False

    def set_rows_to_500(self):
        """Set rows per page to 500 (to show all 296 stocks on one page)"""
        logger.info("Setting rows per page to 500...")

        try:
            time.sleep(2)

            # Find "50 Rows" or "500 Rows" dropdown
            rows_selectors = [
                "//button[contains(text(), 'Rows')]",
                "//button[contains(@id, 'dropdown') and contains(., 'Rows')]",
                "//*[contains(text(), 'Page')]/following-sibling::*/button[contains(., 'Rows')]",
            ]

            rows_button = None
            for selector in rows_selectors:
                try:
                    rows_button = self.driver.find_element(By.XPATH, selector)
                    button_text = rows_button.text
                    logger.info(f"   Found rows button: {button_text}")

                    # If already 500 rows, we're good!
                    if "500 Rows" in button_text:
                        logger.info("   Already set to 500 rows")
                        return True

                    break
                except:
                    continue

            if rows_button is None:
                logger.warning("Could not find rows button")
                return False

            # Click rows dropdown
            rows_button.click()
            time.sleep(2)

            # Find and click "500 Rows" option
            rows_500_selectors = [
                "//button[text()='500 Rows']",
                "//*[contains(text(), '500 Rows')]",
                "//li[contains(text(), '500')]",
                "//button[contains(text(), '500')]",
            ]

            for selector in rows_500_selectors:
                try:
                    rows_500_option = self.driver.find_element(By.XPATH, selector)
                    rows_500_option.click()
                    logger.info("   Changed to 500 rows")
                    time.sleep(3)
                    return True
                except:
                    continue

            logger.error("   Could not find 500 rows option")
            return False

        except Exception as e:
            logger.error(f"   Failed to set rows: {e}")
            return False

    def navigate_to_screener(self):
        """Navigate to Stock Screener"""
        logger.info("=" * 50)
        logger.info("STEP 2: Navigating to Stock Screener")
        logger.info("=" * 50)

        try:
            screener_url = "https://stockanalysis.com/stocks/screener/"

            logger.info(f"Going to: {screener_url}")
            self.driver.get(screener_url)

            # Wait for page to load
            time.sleep(8)

            # Check if we're on the right page
            current_url = self.driver.current_url
            logger.info(f"Current URL: {current_url}")

            if "screener" not in current_url:
                logger.error(f"Unexpected URL: {current_url}")
                return False

            logger.info("Arrived at Stock Screener page")

            # STEP 1: Set country to Sri Lanka
            if not self.set_country_to_sri_lanka():
                logger.error("Failed to set country, but continuing...")

            # STEP 2: Set rows to 500 (to show all 296 stocks on one page!)
            if not self.set_rows_to_500():
                logger.error("Failed to set rows, but continuing...")

            logger.info("Screener configured correctly")
            return True

        except Exception as e:
            logger.error(f"Failed to navigate: {e}")
            return False

    def click_view_tab(self, view_name):
        """Click a view tab (view_0, view_1, etc.)"""
        logger.info(f"   Clicking {view_name} tab...")

        try:
            time.sleep(2)

            # Try multiple selectors for the tab
            tab_selectors = [
                f"//button[contains(text(), '{view_name}')]",
                f"//button[text()='{view_name}']",
                f"//a[contains(text(), '{view_name}')]",
                f"//*[contains(text(), '{view_name}') and (@role='tab' or name()='button')]",
            ]

            tab_element = None
            for selector in tab_selectors:
                try:
                    tab_element = self.driver.find_element(By.XPATH, selector)
                    logger.info(f"      Found {view_name}")
                    break
                except:
                    continue

            if tab_element is None:
                logger.error(f"   Could not find {view_name} tab")
                return False

            # Click the tab (with JavaScript fallback)
            try:
                tab_element.click()
            except:
                self.driver.execute_script("arguments[0].click();", tab_element)

            # Wait for view to load
            time.sleep(6)
            logger.info(f"   Loaded {view_name}")
            return True

        except Exception as e:
            logger.error(f"   Failed to click {view_name}: {e}")
            return False

    def extract_single_page(self, view_name):
        """Extract single page for a view (all 296 stocks on one page!)"""
        try:
            # Wait for data to load
            time.sleep(3)

            # Get page HTML
            html = self.driver.page_source

            # Save HTML file
            filename = f"{view_name}.html"
            filepath = os.path.join(config.HTML_DIR, filename)

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html)

            logger.info(f"   Saved: {filename}")
            return True

        except Exception as e:
            logger.error(f"   Failed to save {view_name}: {e}")
            return False

    def collect_all_views(self):
        """Collect all 13 views (view_0 through view_12)"""
        logger.info("=" * 50)
        logger.info("STEP 3: Collecting All Views")
        logger.info("=" * 50)
        logger.info(f"Total views: {config.TOTAL_VIEWS}")
        logger.info("Each view: ALL 296 stocks on ONE page")
        logger.info("Total parameters: 235 across all views")
        logger.info("=" * 50)

        for i, view_name in enumerate(config.VIEWS):
            logger.info(f"\n[{i+1}/{config.TOTAL_VIEWS}] Processing {view_name}...")

            # Click the view tab
            if not self.click_view_tab(view_name):
                logger.error(f"Failed to load {view_name}")
                return False

            # Extract the page (all stocks on one page!)
            if not self.extract_single_page(view_name):
                logger.error(f"Failed to save {view_name}")
                return False

        logger.info("\n" + "=" * 50)
        logger.info(f"ALL {config.TOTAL_VIEWS} VIEWS COLLECTED!")
        logger.info("=" * 50)
        return True

    def run(self):
        """Main execution flow"""
        try:
            # Step 1: Login
            if not self.login():
                return False

            # Step 2: Navigate to screener and configure
            if not self.navigate_to_screener():
                return False

            # Step 3: Collect all 13 views (NO pagination!)
            if not self.collect_all_views():
                return False

            logger.info("\n" + "=" * 50)
            logger.info("DATA COLLECTION COMPLETE!")
            logger.info("=" * 50)
            logger.info(f"Location: {config.HTML_DIR}")
            logger.info(f"Files: {config.TOTAL_VIEWS} HTML files")
            logger.info(f"Stocks: 296 per file")
            logger.info(f"Parameters: 235 total")
            logger.info("=" * 50)
            return True

        except Exception as e:
            logger.error(f"Collection failed: {e}")
            return False

        finally:
            if self.driver:
                logger.info("\nClosing browser...")
                time.sleep(2)
                self.driver.quit()

def main():
    """Main entry point"""
    collector = StockAnalysisCollector()
    success = collector.run()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
