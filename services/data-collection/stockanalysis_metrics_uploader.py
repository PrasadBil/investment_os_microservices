#!/usr/bin/env python3
"""
Supabase Auto-Upload Script
Automatically uploads processed StockAnalysis data to Supabase database

Migration: Phase 2 (Feb 2026)
- Replaced: supabase.create_client + os.getenv -> common.database.get_supabase_client()
- Removed: manual SUPABASE_URL/KEY environment variable handling
- Original: /opt/selenium_automation/upload_to_supabase.py
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd

# === Investment OS Common Library (Phase 2 Migration) ===
from common.database import get_supabase_client
from common.logging_config import setup_logging

# Setup logging with file output for production
logger = setup_logging('stockanalysis_metrics_uploader', log_to_file=True)

class SupabaseUploader:
    def __init__(self):
        """Initialize Supabase client (Phase 2 Migration)"""
        self.client = get_supabase_client()
        self.table_name = 'cse_complete_data'

    def upload_from_csv(self, csv_file: str):
        """Upload data from CSV file to Supabase"""
        logger.info("=" * 70)
        logger.info("UPLOADING TO SUPABASE")
        logger.info("=" * 70)

        if not os.path.exists(csv_file):
            logger.error(f"CSV file not found: {csv_file}")
            return False

        try:
            # Read CSV
            logger.info(f"Reading CSV: {csv_file}")
            df = pd.read_csv(csv_file)
            logger.info(f"   Rows: {len(df)}")
            logger.info(f"   Columns: {len(df.columns)}")

           # Define large value columns (no capping)
            large_value_columns = {
                'price', 'volume', 'low52', 'marketcap', 'enterprisevalue',
                'bvpershare', 'revperemployee', 'profitperemployee',
                'total_assets', 'total_liabilities', 'total_equity', 'working_capital_view10',
                'netincome', 'operatingcf', 'fcf', 'adjustedfcf', 'fcfpershare',
                'netcf', 'investingcf', 'financingcf', 'capex',
                'revenue', 'grossprofit', 'operatingincome', 'ebit', 'ebitda',
                'total_cash', 'total_debt', 'net_cash', 'tangible_book_value'
            }

            # Cap values in DECIMAL(8,4) columns
            logger.info("Capping extreme values...")
            capped_count = 0

            for col in df.columns:
                if col not in large_value_columns and pd.api.types.is_numeric_dtype(df[col]):
                    # Cap at +/-9999.9999
                    mask = (df[col] > 9999.9999) | (df[col] < -9999.9999)
                    if mask.any():
                        capped_count += mask.sum()
                        df.loc[df[col] > 9999.9999, col] = 9999.9999
                        df.loc[df[col] < -9999.9999, col] = -9999.9999

            logger.info(f"   Capped {capped_count} extreme values")

            # Convert DataFrame to list of dicts
            records = df.to_dict('records')

            # Clean records (convert NaN to None)
            logger.info("Cleaning data...")
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None

            # Upload in batches
            batch_size = 100
            total_batches = (len(records) + batch_size - 1) // batch_size

            logger.info(f"Uploading {len(records)} records in {total_batches} batches...")

            success_count = 0
            error_count = 0

            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                batch_num = (i // batch_size) + 1

                try:
                    # Upsert batch (insert or update if exists)
                    response = self.client.table(self.table_name).upsert(
                        batch,
                        on_conflict='symbol,data_date'
                    ).execute()

                    success_count += len(batch)
                    logger.info(f"   Batch {batch_num}/{total_batches}: {len(batch)} records uploaded")

                except Exception as e:
                    error_count += len(batch)
                    logger.error(f"   Batch {batch_num}/{total_batches} failed: {e}")

            # Summary
            logger.info("")
            logger.info("=" * 70)
            logger.info("UPLOAD SUMMARY")
            logger.info("=" * 70)
            logger.info(f"Success: {success_count} records")
            logger.info(f"Errors: {error_count} records")
            logger.info(f"Total: {len(records)} records")
            logger.info(f"Capped: {capped_count} extreme values")

            if error_count == 0:
                logger.info("ALL DATA UPLOADED SUCCESSFULLY!")
                return True
            else:
                logger.warning(f"{error_count} records failed to upload")
                return False

        except Exception as e:
            logger.error(f"Upload failed: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python3 stockanalysis_metrics_uploader.py <csv_file>")
        print("Example: python3 stockanalysis_metrics_uploader.py output/2025-12-26/cleaned_data.csv")
        sys.exit(1)

    csv_file = sys.argv[1]

    try:
        uploader = SupabaseUploader()
        success = uploader.upload_from_csv(csv_file)

        if success:
            logger.info("Supabase upload completed successfully")
            sys.exit(0)
        else:
            logger.error("Supabase upload failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
