
#!/usr/bin/env python3
"""
CSE Price Processor - STEP 2: Process CSV
Cleans and standardizes downloaded CSV
Runtime: ~5 seconds

Migration: Phase 2 (Feb 2026)
- Changed: /tmp/ paths -> SCRIPT_DIR/temp/ paths
- Original: /opt/selenium_automation/cse_processor.py
"""

import os
import sys
from datetime import datetime
import pandas as pd
import logging

# === Path setup (Phase 2 Migration) ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(SCRIPT_DIR, 'temp')
os.makedirs(TEMP_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CSEProcessor:
    """Process downloaded CSV"""

    def __init__(self):
        today = datetime.now().strftime('%Y-%m-%d')
        self.output_dir = os.path.join('output', today)
        os.makedirs(self.output_dir, exist_ok=True)

    def process_csv(self, raw_csv):
        """Clean and standardize CSV"""
        logger.info("=" * 70)
        logger.info("PROCESSING CSE CSV")
        logger.info("=" * 70)
        logger.info(f"Input: {os.path.basename(raw_csv)}")

        try:
            # Read CSV
            df = pd.read_csv(raw_csv)
            logger.info(f"Original rows: {len(df)}")
            logger.info(f"Original columns: {list(df.columns)}")

            # Clean column names
            df.columns = df.columns.str.strip().str.replace('**', '').str.replace(' (Rs.)', '').str.replace(' (%)', '')

            # Handle duplicate "Change" columns
            new_columns = []
            change_count = 0
            for col in df.columns:
                if col == 'Change':
                    new_columns.append('change_rs' if change_count == 0 else 'change_pct')
                    change_count += 1
                elif col == 'Company Name':
                    new_columns.append('company_name')
                elif col == 'Symbol':
                    new_columns.append('symbol')
                elif col == 'Share Volume':
                    new_columns.append('share_volume')
                elif col == 'Trade Volume':
                    new_columns.append('trade_volume')
                elif col == 'Previous Close':
                    new_columns.append('previous_close')
                elif col == 'Open':
                    new_columns.append('open')
                elif col == 'High':
                    new_columns.append('high')
                elif col == 'Low':
                    new_columns.append('low')
                elif col == 'Last Trade':
                    new_columns.append('price')
                else:
                    new_columns.append(col.lower().replace(' ', '_'))

            df.columns = new_columns

            # Add metadata
            df['collection_date'] = datetime.now().strftime('%Y-%m-%d')
            df['collection_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Remove duplicates
            df = df.drop_duplicates(subset=['symbol'], keep='first')

            # Select final columns (NO 'time' - only collection_timestamp)
            final_columns = ['symbol', 'price', 'open', 'high', 'low', 'previous_close',
                           'change_rs', 'change_pct', 'share_volume', 'trade_volume',
                           'collection_date', 'collection_timestamp']
            final_columns = [c for c in final_columns if c in df.columns]
            df = df[final_columns]

            # Save
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"cse_prices_{timestamp}.csv"
            filepath = os.path.join(self.output_dir, filename)

            df.to_csv(filepath, index=False)

            logger.info(f"Processed: {filename}")
            logger.info(f"Final rows: {len(df)}")
            logger.info(f"Final columns: {list(df.columns)}")

            # Show sample
            logger.info("\nSample (first 5):")
            logger.info(df.head(5).to_string(index=False))

            return filepath

        except Exception as e:
            logger.error(f"Processing failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self, raw_csv=None):
        """Main execution"""
        logger.info("=" * 70)
        logger.info("CSE PROCESSOR - STEP 2")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        # Get raw CSV path (Phase 2: temp/ instead of /tmp/)
        if not raw_csv:
            try:
                temp_file = os.path.join(TEMP_DIR, 'latest_cse_raw.txt')
                with open(temp_file, 'r') as f:
                    raw_csv = f.read().strip()
            except:
                logger.error("No raw CSV file found")
                logger.error("   Run cse_collector.py first")
                return None

        if not os.path.exists(raw_csv):
            logger.error(f"File not found: {raw_csv}")
            return None

        processed_csv = self.process_csv(raw_csv)

        if processed_csv:
            # Save path for uploader (Phase 2: temp/ instead of /tmp/)
            temp_file = os.path.join(TEMP_DIR, 'latest_cse_prices.txt')
            with open(temp_file, 'w') as f:
                f.write(processed_csv)

            logger.info("")
            logger.info("=" * 70)
            logger.info("PROCESSING COMPLETE!")
            logger.info("=" * 70)
            logger.info(f"File: {processed_csv}")
            return processed_csv
        else:
            return None


def main():
    processor = CSEProcessor()
    result = processor.run()
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
