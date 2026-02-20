"""
Enhanced Data Processor - Now includes view_10 balance sheet extraction
Version: 2.0
Date: December 27, 2025

Migration: Phase 2 (Feb 2026)
- CLEAN COPY: No database imports, pure computation (pandas/BeautifulSoup)
- Original: /opt/selenium_automation/data_processor.py
"""

from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import os

class DataProcessor:
    def __init__(self, html_dir=None, output_dir=None):
        self.html_dir = html_dir or "html_files"
        self.output_dir = output_dir or "output"
        self.dataframes = {}

    def parse_value(self, val):
        """
        Convert values with B/T/M/K suffixes to numbers
        Examples:
          "889.71B" -> 889710000000
          "2.13T" -> 2130000000000
          "-152.30B" -> -152300000000
        """
        if pd.isna(val) or val is None:
            return None

        val_str = str(val).strip()

        # Handle empty or dash
        if val_str == '' or val_str == '-':
            return None

        # Extract number and suffix
        match = re.match(r'([+-]?\d+\.?\d*)\s*([BTMK]?)', val_str, re.IGNORECASE)
        if not match:
            # Try to parse as plain number
            try:
                return float(val_str.replace(',', ''))
            except:
                return None

        number, suffix = match.groups()
        number = float(number)

        # Apply multiplier
        multipliers = {
            'T': 1_000_000_000_000,
            'B': 1_000_000_000,
            'M': 1_000_000,
            'K': 1_000,
            '': 1
        }

        multiplier = multipliers.get(suffix.upper(), 1)
        return number * multiplier

    def clean_numeric_value(self, val):
        """Clean numeric values - remove commas, %, $, etc."""
        if pd.isna(val) or val is None:
            return None

        val_str = str(val).strip()

        if val_str == '' or val_str == '-':
            return None

        # Remove common formatting
        val_str = val_str.replace(',', '').replace('$', '').replace('%', '')

        try:
            return float(val_str)
        except:
            return None

    def extract_table_from_html(self, html_file, view_number):
        """Extract table data from HTML file"""
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')

        if not table:
            print(f"  No table found in {html_file}")
            return None

        # Extract headers
        headers = []
        thead = table.find('thead')
        if thead:
            for th in thead.find_all('th'):
                header_id = th.get('id', '')
                header_text = th.get_text(strip=True)
                headers.append(header_id if header_id else header_text)
        else:
            print(f"  No thead found in {html_file}")
            return None

        # Extract data rows
        rows_data = []
        tbody = table.find('tbody')
        if tbody:
            for tr in tbody.find_all('tr'):
                row_data = []
                for td in tr.find_all('td'):
                    text = td.get_text(strip=True)
                    # Keep raw text for now - we'll clean it later
                    row_data.append(text if text and text != '-' else None)
                if row_data:
                    rows_data.append(row_data)

        if not rows_data:
            print(f"  No data rows found in {html_file}")
            return None

        # Create DataFrame
        df = pd.DataFrame(rows_data, columns=headers[:len(rows_data[0])])

        print(f"  View {view_number}: {len(df)} stocks, {len(df.columns)} columns")

        return df

    def process_view_10_balance_sheet(self, df_view10):
        """
        Process view_10 for ALL balance sheet and debt data
        Handles B/T/M suffix conversion
        """
        print("\n  Processing view_10 (balance sheet + debt data)...")

        # Columns that need B/T/M parsing (all large value columns)
        large_value_cols = [
            'cash', 'netCash', 'debt',
            'assets', 'liabilities', 'equity',
            'workingCapital', 'netWorkingCapital',
            'tangibleBookValue'
        ]

        # Columns that are percentages/ratios (clean but don't parse)
        ratio_cols = [
            'debtGrowthQoQ', 'netCashGrowth', 'debtGrowth3Y',
            'debtGrowth', 'debtGrowth5Y', 'netCashByMarketCap',
            'bvPerShare', 'tangibleBookValuePerShare',
            'currentRatio', 'debtEbitda'
        ]

        df_processed = df_view10.copy()

        # Parse large value columns (with B/T/M suffixes)
        for col in large_value_cols:
            if col in df_processed.columns:
                parsed_col = col + '_parsed'
                df_processed[parsed_col] = df_processed[col].apply(self.parse_value)

                # Count successful parses
                parsed_count = df_processed[parsed_col].notna().sum()
                print(f"    {col:25s}: {parsed_count}/{len(df_processed)} parsed")

        # Create final columns with standard names
        df_final = df_processed[['s']].copy()
        df_final.rename(columns={'s': 'symbol'}, inplace=True)

        # Map large value columns to standard names
        column_mapping = {
            'assets_parsed': 'total_assets',
            'liabilities_parsed': 'total_liabilities',
            'equity_parsed': 'total_equity',
            'workingCapital_parsed': 'working_capital_view10',
            'netWorkingCapital_parsed': 'net_working_capital',
            'cash_parsed': 'total_cash',
            'debt_parsed': 'total_debt',
            'netCash_parsed': 'net_cash',
            'tangibleBookValue_parsed': 'tangible_book_value',
        }

        for source_col, target_col in column_mapping.items():
            if source_col in df_processed.columns:
                df_final[target_col] = df_processed[source_col]

        # Add ratio/percentage columns (clean as numeric, no B/T/M parsing needed)
        for col in ratio_cols:
            if col in df_processed.columns:
                df_final[col] = df_processed[col].apply(self.clean_numeric_value)

        print(f"  View 10 processed: {len(df_final.columns)-1} columns extracted")

        return df_final

    def process_all_views(self, date_str):
        """Process all HTML views for a given date"""
        print("=" * 80)
        print(f"PROCESSING DATA FOR: {date_str}")
        print("=" * 80)

        html_date_dir = os.path.join(self.html_dir, date_str)

        if not os.path.exists(html_date_dir):
            print(f"Directory not found: {html_date_dir}")
            return False

        # Process views 0-12 (adjust range if you have different views)
        for view_num in range(13):
            html_file = os.path.join(html_date_dir, f'view_{view_num}.html')

            if not os.path.exists(html_file):
                print(f"  Skipping view_{view_num} (file not found)")
                continue

            df = self.extract_table_from_html(html_file, view_num)

            if df is not None:
                # Special handling for view_10 (balance sheet)
                if view_num == 10:
                    df = self.process_view_10_balance_sheet(df)

                self.dataframes[f'view_{view_num}'] = df

        print(f"\nExtracted {len(self.dataframes)} views")
        return True

    def merge_all_dataframes(self):
        """Merge all views into a single comprehensive dataset"""
        print("\n" + "=" * 80)
        print("MERGING ALL VIEWS")
        print("=" * 80)

        if not self.dataframes:
            print("No dataframes to merge")
            return None

        # Start with first available view
        first_key = list(self.dataframes.keys())[0]
        df_merged = self.dataframes[first_key].copy()

        # Determine the key column (usually 's' or 'symbol')
        key_col = 'symbol' if 'symbol' in df_merged.columns else 's'

        print(f"  Starting with {first_key}: {len(df_merged)} stocks, {len(df_merged.columns)} columns")

        # Merge remaining views
        for view_name, df in self.dataframes.items():
            if view_name == first_key:
                continue

            # Determine key column for this df
            df_key_col = 'symbol' if 'symbol' in df.columns else 's'

            # Merge
            before_cols = len(df_merged.columns)
            df_merged = df_merged.merge(df, left_on=key_col, right_on=df_key_col, how='outer', suffixes=('', f'_{view_name}'))

            # Remove duplicate key columns
            if df_key_col != key_col and f'{df_key_col}' in df_merged.columns:
                df_merged = df_merged.drop(df_key_col, axis=1)

            new_cols = len(df_merged.columns) - before_cols
            print(f"  + {view_name}: added {new_cols} columns (total: {len(df_merged.columns)})")

        # Ensure consistent key column name
        if 's' in df_merged.columns and 'symbol' not in df_merged.columns:
            df_merged.rename(columns={'s': 'symbol'}, inplace=True)
        elif 's' in df_merged.columns and 'symbol' in df_merged.columns:
            df_merged = df_merged.drop('s', axis=1)

        print(f"\nFinal merged data: {len(df_merged)} stocks, {len(df_merged.columns)} columns")

        return df_merged

    def calculate_derived_metrics(self, df):
        """Calculate derived metrics from available data"""
        print("\n" + "=" * 80)
        print("CALCULATING DERIVED METRICS")
        print("=" * 80)

        df = df.copy()

        # CRITICAL: Parse B/T/M suffixes in ALL columns before calculations
        print("\n  Parsing B/T/M suffixes in all columns...")
        columns_parsed = 0

        for col in df.columns:
            # Skip if already numeric
            if df[col].dtype in ['int64', 'float64']:
                continue

            # Check if column might have B/T/M values
            if df[col].dtype == 'object':
                # Sample first 10 non-null values
                sample = df[col].dropna().head(10)

                # Check if any values end with B, T, M, K
                has_suffix = False
                for val in sample:
                    val_str = str(val).strip().upper()
                    if val_str and val_str[-1] in ['B', 'T', 'M', 'K']:
                        # Make sure it's actually a number with suffix (not just a word ending in B/T/M/K)
                        if any(c.isdigit() for c in val_str[:-1]):
                            has_suffix = True
                            break

                if has_suffix:
                    # Parse this column
                    df[col] = df[col].apply(self.parse_value)
                    columns_parsed += 1

        print(f"     Parsed {columns_parsed} columns with B/T/M suffixes")

        # Helper function to convert column to numeric
        def to_numeric_safe(series):
            """Safely convert series to numeric, handling strings"""
            return pd.to_numeric(series, errors='coerce')

        # 1. Revenue (if not present) - from Market Cap / PS Ratio
        if 'revenue' not in df.columns and 'market_cap' in df.columns and 'ps_ratio' in df.columns:
            print("  Calculating Revenue (Market Cap / PS Ratio)...")
            df['revenue'] = to_numeric_safe(df['market_cap']) / to_numeric_safe(df['ps_ratio'])
            revenue_count = df['revenue'].notna().sum()
            print(f"    Coverage: {revenue_count}/{len(df)} ({revenue_count/len(df)*100:.1f}%)")

        # 2. Asset Turnover - Revenue / Total Assets
        if 'revenue' in df.columns and 'total_assets' in df.columns:
            print("  Calculating Asset Turnover (Revenue / Total Assets)...")
            revenue_numeric = to_numeric_safe(df['revenue'])
            assets_numeric = to_numeric_safe(df['total_assets'])
            df['asset_turnover'] = revenue_numeric / assets_numeric
            at_count = df['asset_turnover'].notna().sum()
            print(f"    Coverage: {at_count}/{len(df)} ({at_count/len(df)*100:.1f}%)")

        # 3. Capex Intensity - Capex / Revenue (as percentage)
        if 'capex' in df.columns and 'revenue' in df.columns:
            print("  Calculating Capex Intensity (Capex / Revenue * 100)...")
            capex_numeric = to_numeric_safe(df['capex'])
            revenue_numeric = to_numeric_safe(df['revenue'])
            df['capex_intensity'] = (capex_numeric / revenue_numeric) * 100
            ci_count = df['capex_intensity'].notna().sum()
            print(f"    Coverage: {ci_count}/{len(df)} ({ci_count/len(df)*100:.1f}%)")

        # 4. FCF Margin - FCF / Revenue (as percentage)
        if 'fcf' in df.columns and 'revenue' in df.columns:
            print("  Calculating FCF Margin (FCF / Revenue * 100)...")
            fcf_numeric = to_numeric_safe(df['fcf'])
            revenue_numeric = to_numeric_safe(df['revenue'])
            df['fcf_margin_calc'] = (fcf_numeric / revenue_numeric) * 100

            # Use calculated if original is missing
            if 'fcf_margin' in df.columns:
                existing_fcf = to_numeric_safe(df['fcf_margin'])
                df['fcf_margin'] = existing_fcf.fillna(df['fcf_margin_calc'])
            else:
                df['fcf_margin'] = df['fcf_margin_calc']

            fcf_count = df['fcf_margin'].notna().sum()
            print(f"    Coverage: {fcf_count}/{len(df)} ({fcf_count/len(df)*100:.1f}%)")

        # 5. Debt/Assets - Total Debt / Total Assets (as percentage)
        if 'total_debt' in df.columns and 'total_assets' in df.columns:
            print("  Calculating Debt/Assets (Total Debt / Total Assets * 100)...")
            debt_numeric = to_numeric_safe(df['total_debt'])
            assets_numeric = to_numeric_safe(df['total_assets'])
            df['debt_to_assets'] = (debt_numeric / assets_numeric) * 100
            da_count = df['debt_to_assets'].notna().sum()
            print(f"    Coverage: {da_count}/{len(df)} ({da_count/len(df)*100:.1f}%)")

        print(f"\nDerived metrics calculated")

        return df

    def save_outputs(self, df, date_str):
        """Save processed data to CSV and SQL files"""
        print("\n" + "=" * 80)
        print("SAVING OUTPUTS")
        print("=" * 80)

        output_date_dir = os.path.join(self.output_dir, date_str)
        os.makedirs(output_date_dir, exist_ok=True)

        # Add metadata
        df['data_date'] = date_str
        df['last_updated'] = datetime.now().isoformat()

        # Save CSV
        csv_file = os.path.join(output_date_dir, 'cleaned_data.csv')

        # Remove commas and % signs from all numeric columns
        for col in df.columns:
            if df[col].dtype == 'object':
                # Remove commas and percent signs from string numbers
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = df[col].astype(str).str.replace('%', '', regex=False)
                df[col] = df[col].replace('nan', None)
                df[col] = df[col].replace('', None)

        # Convert column names to lowercase to match PostgreSQL/Supabase
        df.columns = df.columns.str.lower()
        df.to_csv(csv_file, index=False)
        print(f"  CSV saved: {csv_file}")
        print(f"     {len(df)} stocks, {len(df.columns)} columns")

        # Save SQL (optional - for reference)
        sql_file = os.path.join(output_date_dir, f'import_cse_{date_str.replace("-", "")}.sql')

        # Simple SQL generation (you can enhance this)
        with open(sql_file, 'w') as f:
            f.write(f"-- CSE Data Import\n")
            f.write(f"-- Generated: {datetime.now().isoformat()}\n")
            f.write(f"-- Date: {date_str}\n")
            f.write(f"-- Stocks: {len(df)}\n\n")
            f.write(f"-- Note: Use Supabase upload script for actual import\n")

        print(f"  SQL reference saved: {sql_file}")

        return csv_file

    def generate_report(self, df):
        """Generate data quality report"""
        print("\n" + "=" * 80)
        print("DATA QUALITY REPORT")
        print("=" * 80)

        print(f"\nTotal Stocks: {len(df)}")
        print(f"Total Columns: {len(df.columns)}")

        # Key metrics coverage
        key_metrics = {
            'Balance Sheet': ['total_assets', 'total_liabilities', 'total_equity', 'working_capital'],
            'Operations': ['revenue', 'operating_cf', 'fcf', 'capex'],
            'Efficiency': ['asset_turnover', 'capex_intensity', 'fcf_margin'],
            'Profitability': ['roa', 'roe', 'roic', 'profit_margin'],
        }

        for category, cols in key_metrics.items():
            print(f"\n{category}:")
            for col in cols:
                if col in df.columns:
                    count = df[col].notna().sum()
                    pct = (count / len(df)) * 100
                    status = "PASS" if pct >= 90 else "WARN" if pct >= 70 else "FAIL"
                    print(f"  {status} {col:25s}: {count:3d}/{len(df)} ({pct:5.1f}%)")

def main():
    """Main execution function"""

    # Configuration
    HTML_DIR = "html_files"  # Adjust to your actual path
    OUTPUT_DIR = "output"     # Adjust to your actual path
    DATE_STR = datetime.now().strftime("%Y-%m-%d")   # Adjust to your actual date

    print("=" * 80)
    print("ENHANCED DATA PROCESSOR v2.0")
    print("Now includes view_10 balance sheet extraction!")
    print("=" * 80)

    # Initialize processor
    processor = DataProcessor(HTML_DIR, OUTPUT_DIR)

    # Process all views
    success = processor.process_all_views(DATE_STR)

    if not success:
        print("\nProcessing failed")
        return

    # Merge all dataframes
    df_merged = processor.merge_all_dataframes()

    if df_merged is None:
        print("\nMerge failed")
        return

    # Calculate derived metrics
    df_final = processor.calculate_derived_metrics(df_merged)

    # Save outputs
    csv_file = processor.save_outputs(df_final, DATE_STR)

    # Generate report
    processor.generate_report(df_final)

    print("\n" + "=" * 80)
    print("DATA PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"\nOutput file: {csv_file}")
    print(f"Ready for Supabase upload!")

if __name__ == "__main__":
    main()
