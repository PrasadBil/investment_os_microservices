"""
Configuration file for StockAnalysis.com data collector

Migration: Phase 2 (Feb 2026)
- Renamed: config.py -> stockanalysis_config.py (avoid collision with common.config)
- Changed: Hardcoded credentials -> environment variables (STOCKANALYSIS_EMAIL, STOCKANALYSIS_PASSWORD)
- NOTE: Add STOCKANALYSIS_EMAIL and STOCKANALYSIS_PASSWORD to /opt/investment-os/.env
- Original: /opt/selenium_automation/config.py
"""

import os
from datetime import datetime

# ============================================
# CREDENTIALS (Phase 2: from environment variables)
# ============================================
EMAIL = os.getenv('STOCKANALYSIS_EMAIL', '')
PASSWORD = os.getenv('STOCKANALYSIS_PASSWORD', '')

# ============================================
# URLS
# ============================================
BASE_URL = "https://stockanalysis.com/stocks/screener/"
LOGIN_URL = "https://stockanalysis.com/login/"

# ============================================
# DIRECTORIES
# ============================================
# Base directory (where this script is located)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Data directories
today = datetime.now().strftime("%Y-%m-%d")
HTML_DIR = os.path.join(BASE_DIR, "html_files", today)
OUTPUT_DIR = os.path.join(BASE_DIR, "output", today)
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Create directories if they don't exist
os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ============================================
# FILES
# ============================================
LOG_FILE = os.path.join(LOG_DIR, f"{today}.log")
RAW_CSV = os.path.join(OUTPUT_DIR, "raw_data.csv")
CLEANED_CSV = os.path.join(OUTPUT_DIR, "cleaned_data.csv")
SQL_FILE = os.path.join(OUTPUT_DIR, f"import_cse_{today.replace('-', '')}.sql")

# ============================================
# SCRAPING SETTINGS
# ============================================
PAGE_LOAD_WAIT = 5  # seconds to wait for page load
CLICK_WAIT = 2      # seconds to wait after clicking
LOGIN_WAIT = 5      # seconds to wait after login

# ============================================
# VIEW SETTINGS (Updated - 13 views, no pagination!)
# ============================================
# You now have 13 views covering all 235 parameters
# Each view has <20 parameters, so all 296 stocks fit on ONE page
VIEWS = [
    "view_0",
    "view_1",
    "view_2",
    "view_3",
    "view_4",
    "view_5",
    "view_6",
    "view_7",
    "view_8",
    "view_9",
    "view_10",
    "view_11",
    "view_12"
]

TOTAL_VIEWS = len(VIEWS)  # 13 views total
PAGES_PER_VIEW = 1  # Only 1 page per view (no pagination needed!)

# ============================================
# DATA PROCESSING
# ============================================
ROWS_PER_PAGE = 500     # Set to 500 to show ALL stocks on one page
EXPECTED_TOTAL_STOCKS = 296  # Expected total for Sri Lanka

# ============================================
# SELENIUM SETTINGS
# ============================================
HEADLESS = True         # Phase 2: True for VPS headless operation
CHROME_OPTIONS = [
    '--start-maximized',
    '--disable-blink-features=AutomationControlled',
]

# ============================================
# DATABASE SETTINGS
# ============================================
# Phase 2: Supabase handled by common.database module
TABLE_NAME = "cse_complete_data"

# ============================================
# LOGGING SETTINGS
# ============================================
LOG_LEVEL = "INFO"      # Can be: DEBUG, INFO, WARNING, ERROR
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# ============================================
# PRINT CONFIGURATION (for debugging)
# ============================================
def print_config():
    """Print current configuration"""
    print("=" * 60)
    print("CONFIGURATION SETTINGS")
    print("=" * 60)
    print(f"Email: {EMAIL}")
    print(f"Base URL: {BASE_URL}")
    print(f"HTML Directory: {HTML_DIR}")
    print(f"Output Directory: {OUTPUT_DIR}")
    print(f"Log File: {LOG_FILE}")
    print(f"Total Views: {TOTAL_VIEWS}")
    print(f"Pages per View: {PAGES_PER_VIEW}")
    print(f"Expected Stocks: {EXPECTED_TOTAL_STOCKS}")
    print("=" * 60)

if __name__ == "__main__":
    print_config()
