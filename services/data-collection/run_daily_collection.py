#!/usr/bin/env python3
"""
Daily collection runner for StockAnalysis.com data
Runs the complete collection process and data processing pipeline

Migration: Phase 2 (Feb 2026)
- Changed: import config references -> stockanalysis_config
- CLEAN COPY: No database imports
- Original: /opt/selenium_automation/run_daily_collection.py
"""

import sys
import os
from datetime import datetime

# Import the collector (Phase 2: uses stockanalysis_config internally)
from selenium_collector import StockAnalysisCollector

# Import data processor (if you have it)
try:
    from data_processor import DataProcessor
    HAS_PROCESSOR = True
except ImportError:
    HAS_PROCESSOR = False
    print("data_processor.py not found - will skip CSV/SQL generation")

def main():
    """Run the daily collection"""
    print("=" * 70)
    print("STOCKANALYSIS.COM DAILY DATA COLLECTION")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Step 1: Collect HTML files
    print("\nSTEP 1: COLLECTING HTML FILES")
    print("-" * 70)

    collector = StockAnalysisCollector()
    success = collector.run()

    if not success:
        print("\nCOLLECTION FAILED!")
        print("Check the logs for details.")
        return 1

    print("\nHTML COLLECTION COMPLETE!")

    # Step 2: Process data (if processor is available)
    if HAS_PROCESSOR:
        print("\nSTEP 2: PROCESSING DATA")
        print("-" * 70)

        try:
            processor = DataProcessor()
            processor.process_all()
            print("\nDATA PROCESSING COMPLETE!")
        except Exception as e:
            print(f"\nDATA PROCESSING FAILED: {e}")
            print("HTML files were collected successfully, but CSV/SQL generation failed.")
            return 1
    else:
        print("\nSTEP 2: SKIPPED (no data_processor.py)")

    # Summary
    print("\n" + "=" * 70)
    print("DAILY COLLECTION COMPLETE!")
    print("=" * 70)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nCollection interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
