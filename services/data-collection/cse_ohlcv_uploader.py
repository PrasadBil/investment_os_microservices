
#!/usr/bin/env python3
"""
CSE Price Uploader - Upload to Supabase
Runtime: ~5 seconds

Migration: Phase 2 (Feb 2026)
- Replaced: dotenv/load_dotenv + supabase.create_client -> common.database.get_supabase_client()
- Removed: manual SUPABASE_URL/KEY environment variable handling
- Changed: /tmp/ paths -> SCRIPT_DIR/temp/ paths
- Original: /opt/selenium_automation/cse_uploader.py
"""

import os
import sys
from datetime import datetime
import pandas as pd
import logging

# === Investment OS Common Library (Phase 2 Migration) ===
from common.database import get_supabase_client
from common.logging_config import setup_logging

# Setup logging with file output for production
logger = setup_logging('cse_ohlcv_uploader', log_to_file=True)

# === Path setup (Phase 2 Migration) ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(SCRIPT_DIR, 'temp')
os.makedirs(TEMP_DIR, exist_ok=True)


class CSEPriceUploader:
    """Upload CSE prices to Supabase"""

    def __init__(self):
        # Get Supabase client from common library (Phase 2 Migration)
        self.supabase = get_supabase_client()
        self.table_name = 'cse_daily_prices'

    def upload_prices(self, csv_file):
        """Upload prices to Supabase"""
        logger.info("=" * 70)
        logger.info("UPLOADING CSE PRICES TO SUPABASE")
        logger.info("=" * 70)
        logger.info(f"File: {csv_file}")

        try:
            # Read CSV
            df = pd.read_csv(csv_file)

            logger.info(f"Rows: {len(df)}")
            logger.info(f"Columns: {list(df.columns)}")

            # Rename columns to match Supabase schema
            column_mapping = {
                'company_name': 'company_name',
                'symbol': 'symbol',
                'price': 'price',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'previous_close': 'previous_close',
                'change_rs': 'change_rs',
                'change_pct': 'change_pct',
                'share_volume': 'share_volume',
                'trade_volume': 'trade_volume',
                'collection_date': 'collection_date',
                'collection_timestamp': 'collection_timestamp'
            }

            # Only rename columns that exist
            existing_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
            df.rename(columns=existing_mapping, inplace=True)

            # Add source
            df['source'] = 'CSE_Trade_Summary'

            # Convert to records
            records = df.to_dict('records')

            # Upload in batches
            batch_size = 100
            total_batches = (len(records) + batch_size - 1) // batch_size

            logger.info(f"Uploading {len(records)} prices in {total_batches} batches...")

            success_count = 0
            error_count = 0

            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1

                try:
                    # Upsert to Supabase
                    response = self.supabase.table(self.table_name).upsert(
                        batch,
                        on_conflict='symbol,collection_date'
                    ).execute()

                    success_count += len(batch)
                    logger.info(f"   Batch {batch_num}/{total_batches}: {len(batch)} prices uploaded")

                except Exception as e:
                    error_count += len(batch)
                    logger.error(f"   Batch {batch_num}/{total_batches} failed: {e}")

            # Summary
            logger.info("")
            logger.info("=" * 70)
            logger.info("UPLOAD SUMMARY")
            logger.info("=" * 70)
            logger.info(f"Success: {success_count} prices")
            logger.info(f"Errors: {error_count} prices")
            logger.info(f"Total: {len(records)} prices")

            if error_count == 0:
                logger.info("ALL PRICES UPLOADED SUCCESSFULLY!")

            return error_count == 0

        except Exception as e:
            logger.error(f"Upload failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self, csv_file):
        """Main execution"""
        logger.info("")

        if not os.path.exists(csv_file):
            logger.error(f"CSV file not found: {csv_file}")
            return False

        success = self.upload_prices(csv_file)

        if success:
            logger.info("")
            logger.info("=" * 70)
            logger.info("CSE PRICE UPLOAD COMPLETE!")
            logger.info("=" * 70)
            logger.info(f"Table: {self.table_name}")
            logger.info(f"Runtime: ~5 seconds")
            logger.info("=" * 70)
            return True
        else:
            logger.error("Upload had errors")
            return False


def main():
    """Entry point"""
    try:
        # Get CSV file from temp file (Phase 2: temp/ instead of /tmp/)
        try:
            temp_file = os.path.join(TEMP_DIR, 'latest_cse_prices.txt')
            with open(temp_file, 'r') as f:
                csv_file = f.read().strip()
        except:
            if len(sys.argv) < 2:
                logger.error("Usage: python3 cse_ohlcv_uploader.py <csv_file>")
                logger.error("   OR: Run after cse_ohlcv_processor.py")
                sys.exit(1)
            csv_file = sys.argv[1]

        uploader = CSEPriceUploader()
        success = uploader.run(csv_file)

        sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
