

#!/usr/bin/env python3
"""
Upload composite scores CSV to Supabase

FILE: upload_composite_to_supabase.py
CREATED: 2026-01-04
AUTHOR: Investment OS

VERSION HISTORY:
    v1.0.0  2026-01-04  Initial creation — Supabase uploader (UPSERT method)
    v1.1.0  2026-01-04  Fix: Uses INSERT instead of UPSERT for historical tracking
    v1.1.1  2026-02-12  Migrated to services/scoring-7d (Phase 2 microservices)
                         Replaced dotenv/load_dotenv with common.database.get_supabase_client()
    v1.1.2  2026-02-16  Added version history header (new project standard)
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import argparse

# === Investment OS Common Library (Phase 2 Migration) ===
from common.database import get_supabase_client


def safe_float(val):
    """Convert to float, handling NaN/inf"""
    if pd.isna(val) or np.isinf(val):
        return None
    try:
        return float(val)
    except:
        return None

def safe_int(val):
    """Convert to int, handling NaN"""
    if pd.isna(val):
        return None
    try:
        return int(val)
    except:
        return None

def safe_str(val):
    """Convert to string, handling NaN"""
    if pd.isna(val):
        return None
    return str(val)

def safe_bool(val):
    """Convert to boolean, handling various inputs"""
    if pd.isna(val):
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ['true', 't', 'yes', '1']
    return bool(val)

def extract_date_from_filename(filename):
    """Extract date from filename like composite_scores_20260104_151433.csv"""
    import re
    match = re.search(r'(\d{8})_\d{6}', filename)
    if match:
        date_str = match.group(1)
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return None

def upload_to_supabase(csv_path, scoring_date=None):
    """Upload composite scores to Supabase - INSERT only, no UPSERT"""

    print("=" * 80)
    print("COMPOSITE SCORES - SUPABASE UPLOADER (INSERT MODE)")
    print("=" * 80)
    print()

    # Auto-detect date from filename if not provided
    if not scoring_date:
        scoring_date = extract_date_from_filename(csv_path)
        if scoring_date:
            print(f"Auto-detected date: {scoring_date}")
        else:
            scoring_date = datetime.now().strftime('%Y-%m-%d')
            print(f"Using today's date: {scoring_date}")
    else:
        print(f"Using provided date: {scoring_date}")

    # Load CSV
    print(f"Loading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"   Loaded {len(df)} stocks")

    # Show columns
    print(f"   Columns: {', '.join(df.columns[:5])}...")
    print()

    # Clean column names - handle both formats
    column_mapping = {}
    for col in df.columns:
        # Remove any 'd1_', 'd2_' prefixes
        clean_col = col.replace('d1_', 'dimension1_') \
                       .replace('d2_', 'dimension2_') \
                       .replace('d3_', 'dimension3_') \
                       .replace('d4_', 'dimension4_') \
                       .replace('d5_', 'dimension5_') \
                       .replace('d6_', 'dimension6_') \
                       .replace('d7_', 'dimension7_')
        column_mapping[col] = clean_col

    df = df.rename(columns=column_mapping)

    # Initialize Supabase via common library
    print("Connecting to Supabase...")
    supabase = get_supabase_client()
    print("   Connected")
    print()

    # Prepare records
    print(f"Preparing {len(df)} records for upload...")

    records = []
    current_timestamp = datetime.now().isoformat()

    for _, row in df.iterrows():
        record = {
            'scoring_date': scoring_date,
            'scoring_timestamp': current_timestamp,  # Same timestamp for all records in this batch
            'symbol': safe_str(row['symbol']),
            'sector': safe_str(row.get('sector')),
            'rank': safe_int(row.get('rank')),
            'composite_score': safe_float(row.get('composite_score')),
            'quality_tier': safe_str(row.get('quality_tier')),
            'recommendation': safe_str(row.get('recommendation')),
            'interpretation': safe_str(row.get('interpretation')),

            # Dimension scores
            'dimension1_profitability': safe_float(row.get('dimension1_profitability')),
            'dimension2_financial': safe_float(row.get('dimension2_financial')),
            'dimension3_valuation': safe_float(row.get('dimension3_valuation')),
            'dimension4_growth': safe_float(row.get('dimension4_growth')),
            'dimension5_management': safe_float(row.get('dimension5_management')),
            'dimension6_moat': safe_float(row.get('dimension6_moat')),
            'dimension7_sentiment': safe_float(row.get('dimension7_sentiment')),

            # Watch list flags
            'watch_list_flagged': safe_bool(row.get('watch_list_flagged', False)),
            'watch_list_reason': safe_str(row.get('watch_list_reason')),
            'original_recommendation': safe_str(row.get('original_recommendation'))
        }

        records.append(record)

    print(f"   Prepared {len(records)} records")
    print()

    # Upload in batches - USE INSERT, NOT UPSERT
    print("Uploading to Supabase (INSERT mode - allows multiple uploads per day)...")
    batch_size = 100
    successful = 0
    failed = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            # INSERT only - no upsert!
            response = supabase.table('composite_scores').insert(batch).execute()
            successful += len(batch)
            print(f"   Batch {i//batch_size + 1}: {len(batch)} records uploaded")
        except Exception as e:
            failed += len(batch)
            print(f"   Batch {i//batch_size + 1} failed: {e}")

    print()
    print("=" * 80)
    print("UPLOAD SUMMARY")
    print("=" * 80)
    print(f"Total records: {len(records)}")
    print(f"Successfully uploaded: {successful}")
    print(f"Failed: {failed}")
    print()

    if failed == 0:
        print("All records uploaded successfully!")

        # Verify upload
        print()
        print("Verifying upload...")
        try:
            # Count records for this specific timestamp (not just date!)
            verify_response = supabase.table('composite_scores') \
                .select('*', count='exact') \
                .eq('scoring_timestamp', current_timestamp) \
                .execute()

            count = verify_response.count
            print(f"   Found {count} records with timestamp {current_timestamp}")

            if count == len(records):
                print("   Count matches! Upload verified.")
                print()
                print("Upload complete!")
                return True
            else:
                print(f"   Expected {len(records)}, found {count}")
                print("   Records may have been uploaded but verification uncertain")
                return True
        except Exception as e:
            print(f"   Verification failed: {e}")
            print("   But upload likely succeeded - check Supabase directly")
            return True
    else:
        print(f"{failed} records failed to upload")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload composite scores to Supabase')
    parser.add_argument('--input', required=True, help='Path to composite scores CSV')
    parser.add_argument('--date', help='Scoring date (YYYY-MM-DD), auto-detected if not provided')

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"ERROR: File not found: {args.input}")
        sys.exit(1)

    success = upload_to_supabase(args.input, args.date)
    sys.exit(0 if success else 1)
