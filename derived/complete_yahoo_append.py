"""
Complete Yahoo Finance Data Append (Recovery Script)

Purpose:
    Use the saved checkpoint to complete the append operation with proper timezone handling.

Author: Generated for congressional trading analysis
Date: 2025-11-09
"""

import pandas as pd
from datetime import datetime

# Paths
CHECKPOINT_PATH = "/Users/caleb/Research/congress_trading/data/derived/yahoo_fetch_checkpoint.csv"
FILTERED_STOCK_PATH = "/Users/caleb/Research/congress_trading/data/derived/all_stock_data_filtered.csv"
OUTPUT_STOCK_PATH = "/Users/caleb/Research/congress_trading/data/derived/all_stock_data_filtered_enhanced.csv"


def main():
    """Complete the append operation using checkpoint data."""
    print("="*70)
    print("Recovery: Completing Yahoo Finance Data Append")
    print("="*70)
    
    # Load checkpoint data
    print(f"\nLoading checkpoint data from {CHECKPOINT_PATH}...")
    df_new = pd.read_csv(CHECKPOINT_PATH)
    print(f"  Loaded {len(df_new):,} rows across {df_new['Ticker'].nunique()} unique tickers")
    
    # Parse dates and ensure timezone-naive
    print("  Parsing dates and removing timezone info...")
    df_new["Date"] = pd.to_datetime(df_new["Date"], utc=True).dt.tz_localize(None)
    print(f"  Date range: {df_new['Date'].min()} to {df_new['Date'].max()}")
    
    # Load existing stock data
    print(f"\nLoading existing stock data from {FILTERED_STOCK_PATH}...")
    df_existing = pd.read_csv(FILTERED_STOCK_PATH)
    print(f"  Loaded {len(df_existing):,} rows")
    
    # Parse existing dates
    df_existing["Date"] = pd.to_datetime(df_existing["Date"])
    print(f"  Date range: {df_existing['Date'].min()} to {df_existing['Date'].max()}")
    
    # Combine
    print("\nCombining datasets...")
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    print(f"  Combined: {len(df_combined):,} rows")
    
    # Remove duplicates
    print("\nRemoving duplicates (Date, Ticker)...")
    original_len = len(df_combined)
    df_combined = df_combined.drop_duplicates(subset=["Date", "Ticker"], keep="first")
    duplicates_removed = original_len - len(df_combined)
    print(f"  Removed {duplicates_removed:,} duplicate rows")
    print(f"  Remaining: {len(df_combined):,} rows")
    
    # Sort
    print("\nSorting by Date and Ticker...")
    df_combined = df_combined.sort_values(["Date", "Ticker"]).reset_index(drop=True)
    
    # Verify columns match expected format
    expected_columns = ["Date", "Ticker", "Open", "High", "Low", "Close", 
                       "Volume", "Dividends", "Stock Splits"]
    if list(df_combined.columns) != expected_columns:
        print(f"\nWarning: Column order doesn't match expected format")
        print(f"  Expected: {expected_columns}")
        print(f"  Got: {list(df_combined.columns)}")
        print("  Reordering columns...")
        df_combined = df_combined[expected_columns]
    
    # Save
    print(f"\nSaving enhanced stock data to {OUTPUT_STOCK_PATH}...")
    df_combined.to_csv(OUTPUT_STOCK_PATH, index=False)
    
    # Summary statistics
    print("\n" + "="*70)
    print("APPEND COMPLETE")
    print("="*70)
    print(f"\nFinal dataset statistics:")
    print(f"  Total rows: {len(df_combined):,}")
    print(f"  Unique tickers: {df_combined['Ticker'].nunique():,}")
    print(f"  Date range: {df_combined['Date'].min()} to {df_combined['Date'].max()}")
    print(f"\nOriginal data: {len(df_existing):,} rows")
    print(f"New data added: {len(df_new):,} rows")
    print(f"Duplicates removed: {duplicates_removed:,} rows")
    print(f"Net addition: {len(df_combined) - len(df_existing):,} rows")
    
    print(f"\nEnhanced stock data saved to:")
    print(f"  {OUTPUT_STOCK_PATH}")
    
    print("\n✓ Ready to re-filter congressional trading data with enhanced stock data!")


if __name__ == "__main__":
    main()
