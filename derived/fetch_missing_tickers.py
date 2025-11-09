"""
Fetch Missing Ticker Data from Yahoo Finance

Purpose:
    1. Load list of tickers removed from congressional trading data
    2. Query Yahoo Finance for each ticker to get daily data (2012-2024)
    3. Append successful fetches to the filtered stock data
    4. Generate a report of successful/failed fetches

Author: Generated for congressional trading analysis
Date: 2025-11-09
"""

import pandas as pd
import yfinance as yf
from datetime import datetime
import time
import re
from pathlib import Path

# Global constants
RAW_CONGRESS_PATH = "/Users/caleb/Research/congress_trading/data/raw/congress_trading.csv"
FILTERED_STOCK_PATH = "/Users/caleb/Research/congress_trading/data/derived/all_stock_data_filtered.csv"
OUTPUT_STOCK_PATH = "/Users/caleb/Research/congress_trading/data/derived/all_stock_data_filtered_enhanced.csv"
REPORT_PATH = "/Users/caleb/Research/congress_trading/data/derived/yahoo_fetch_report.md"
START_DATE = "2012-01-01"
END_DATE = "2024-12-31"
BATCH_SIZE = 50
SLEEP_BETWEEN_BATCHES = 2


def is_valid_ticker(ticker):
    """
    Determine if a ticker is potentially valid for Yahoo Finance.
    
    Filters out obvious non-tickers like bonds, CUSIPs, dates, etc.
    """
    if pd.isna(ticker) or ticker == "":
        return False
    
    # Filter out numeric-only tickers
    if ticker.replace(".", "").replace("-", "").isdigit():
        return False
    
    # Filter out dates and maturity terms
    date_keywords = ["DUE", "MATURE", "WEEK", "MONTH", "/"]
    if any(keyword in ticker for keyword in date_keywords):
        return False
    
    # Filter out CUSIP-like identifiers (9+ digits)
    if re.search(r'\d{9,}', ticker):
        return False
    
    # Filter out obvious treasury/bond identifiers
    if ticker.startswith("912") or ticker.startswith("9142"):
        return False
    
    # Filter out entries that are clearly not tickers
    invalid_keywords = [
        "SYMBOL", "TYPE", "DATE", "FUND", "MATURE", "TREASURY",
        "BITCOIN", "RIPPLE", "SOLANA", "TRON", "DUE", "INTEREST",
        "PARTNER", "INVEST", "CORPORAT", "STATE OF", "MONTGOMERY"
    ]
    if any(keyword in ticker for keyword in invalid_keywords):
        return False
    
    # Filter out tickers with foreign exchange suffixes
    foreign_suffixes = [".IL", ".MI", ".TI", ".PA", ".SG", ".V", ".AS", 
                       ".MU", ".F", ".BE", ".SW"]
    if any(ticker.endswith(suffix) for suffix in foreign_suffixes):
        return False
    
    # Filter out percentages and other special characters
    if "%" in ticker or "^" in ticker:
        return False
    
    # Keep ticker if it passes all filters
    return True


def clean_ticker_for_yahoo(ticker):
    """
    Clean ticker symbol for Yahoo Finance API.
    
    Some congressional tickers need conversion (e.g., preferred stocks).
    """
    # Handle preferred stocks (various formats)
    ticker = ticker.replace("$", "-P")
    ticker = ticker.replace(".P", "-P")
    ticker = ticker.replace("-PA", "-P-A")
    ticker = ticker.replace("-PB", "-P-B")
    
    # Handle warrants
    if ticker.endswith("-W") or ticker.endswith(".W"):
        pass  # Keep as is, Yahoo handles these
    
    # Handle class shares
    ticker = ticker.replace(".A", "-A")
    ticker = ticker.replace(".B", "-B")
    ticker = ticker.replace(".C", "-C")
    
    return ticker


def fetch_ticker_data(ticker, start_date, end_date):
    """
    Fetch historical data for a ticker from Yahoo Finance.
    
    Returns DataFrame with columns: Date, Ticker, Open, High, Low, Close, Volume, Dividends, Stock Splits
    """
    try:
        # Download data from Yahoo Finance
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        
        # Check if data was returned
        if hist.empty:
            return None, "No data available"
        
        # Reset index to make Date a column
        hist = hist.reset_index()
        
        # Rename and select columns to match our stock data format
        hist = hist.rename(columns={
            "Date": "Date",
            "Open": "Open",
            "High": "High",
            "Low": "Low",
            "Close": "Close",
            "Volume": "Volume",
            "Dividends": "Dividends",
            "Stock Splits": "Stock Splits"
        })
        
        # Add ticker column
        hist["Ticker"] = ticker.upper()
        
        # Select only columns we need
        columns = ["Date", "Ticker", "Open", "High", "Low", "Close", 
                  "Volume", "Dividends", "Stock Splits"]
        hist = hist[columns]
        
        return hist, None
        
    except Exception as e:
        return None, str(e)


def get_missing_tickers():
    """Load congressional trading data and identify missing tickers."""
    print("Loading congressional trading data...")
    df_congress = pd.read_csv(RAW_CONGRESS_PATH, usecols=["Ticker"])
    df_congress["Ticker"] = df_congress["Ticker"].astype("string").str.upper().str.strip()
    congress_tickers = set(df_congress["Ticker"].dropna().unique())
    
    print("Loading existing stock data...")
    df_stock = pd.read_csv(FILTERED_STOCK_PATH, usecols=["Ticker"], dtype={"Ticker": "string"})
    df_stock["Ticker"] = df_stock["Ticker"].astype("string").str.upper().str.strip()
    stock_tickers = set(df_stock["Ticker"].dropna().unique())
    
    missing_tickers = congress_tickers - stock_tickers
    
    print(f"\nFound {len(missing_tickers)} missing tickers")
    
    return sorted(missing_tickers)


def filter_tickers(tickers):
    """Filter tickers to only those potentially valid for Yahoo Finance."""
    valid = [t for t in tickers if is_valid_ticker(t)]
    invalid = [t for t in tickers if not is_valid_ticker(t)]
    
    print(f"\nFiltered tickers:")
    print(f"  Valid for Yahoo Finance: {len(valid)}")
    print(f"  Invalid/Skipped: {len(invalid)}")
    
    return valid, invalid


def fetch_batch(tickers, start_date, end_date):
    """
    Fetch data for multiple tickers in batches.
    
    Returns dict with results and statistics.
    """
    results = {
        "successful": [],
        "failed": [],
        "data_frames": []
    }
    
    total = len(tickers)
    
    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{total}] Fetching {ticker}...", end=" ")
        
        # Clean ticker for Yahoo Finance
        yahoo_ticker = clean_ticker_for_yahoo(ticker)
        
        # Fetch data
        df, error = fetch_ticker_data(yahoo_ticker, start_date, end_date)
        
        if df is not None and len(df) > 0:
            print(f"✓ ({len(df)} rows)")
            results["successful"].append({
                "original_ticker": ticker,
                "yahoo_ticker": yahoo_ticker,
                "rows": len(df),
                "date_min": df["Date"].min(),
                "date_max": df["Date"].max()
            })
            results["data_frames"].append(df)
        else:
            print(f"✗ ({error})")
            results["failed"].append({
                "ticker": ticker,
                "yahoo_ticker": yahoo_ticker,
                "error": error
            })
        
        # Sleep between requests to avoid rate limiting
        if i % BATCH_SIZE == 0 and i < total:
            print(f"\n  Sleeping {SLEEP_BETWEEN_BATCHES}s to avoid rate limiting...")
            time.sleep(SLEEP_BETWEEN_BATCHES)
    
    return results


def append_to_stock_data(new_data_frames, existing_stock_path, output_path):
    """Append newly fetched data to existing stock data."""
    print(f"\nAppending new data to stock file...")
    
    # Combine all new data
    if not new_data_frames:
        print("  No new data to append")
        return
    
    df_new = pd.concat(new_data_frames, ignore_index=True)
    
    print(f"  New data: {len(df_new):,} rows across {df_new['Ticker'].nunique()} tickers")
    
    # Convert timezone-aware dates to timezone-naive to match existing data
    if pd.api.types.is_datetime64_any_dtype(df_new["Date"]):
        if hasattr(df_new["Date"].dtype, 'tz') and df_new["Date"].dtype.tz is not None:
            print("  Converting timezone-aware dates to timezone-naive...")
            df_new["Date"] = df_new["Date"].dt.tz_localize(None)
    
    # Load existing data
    print(f"  Loading existing data from {existing_stock_path}...")
    df_existing = pd.read_csv(existing_stock_path, parse_dates=["Date"])
    
    print(f"  Existing data: {len(df_existing):,} rows")
    
    # Combine
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    
    # Remove duplicates (in case some tickers were already present)
    original_len = len(df_combined)
    df_combined = df_combined.drop_duplicates(subset=["Date", "Ticker"], keep="first")
    duplicates_removed = original_len - len(df_combined)
    
    if duplicates_removed > 0:
        print(f"  Removed {duplicates_removed:,} duplicate rows")
    
    # Sort by date and ticker (convert to string to avoid sorting issues)
    print("  Sorting combined data...")
    df_combined["Date"] = pd.to_datetime(df_combined["Date"])
    df_combined = df_combined.sort_values(["Date", "Ticker"]).reset_index(drop=True)
    
    # Save
    print(f"  Saving to {output_path}...")
    df_combined.to_csv(output_path, index=False)
    
    print(f"  ✓ Saved {len(df_combined):,} total rows")
    
    return df_combined


def write_report(stats, report_path):
    """Generate markdown report of fetching results."""
    
    total_attempted = len(stats["valid_tickers"])
    total_successful = len(stats["successful"])
    total_failed = len(stats["failed"])
    total_skipped = len(stats["invalid_tickers"])
    success_rate = (total_successful / total_attempted * 100) if total_attempted > 0 else 0
    
    total_rows_added = sum(s["rows"] for s in stats["successful"])
    
    report = f"""# Yahoo Finance Fetch Report: Missing Congressional Trading Tickers

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Summary

This report documents the attempt to recover missing ticker data from Yahoo Finance for congressional trades that were initially excluded due to missing stock data.

### Fetch Statistics

- **Total missing tickers**: {stats['total_missing']:,}
- **Skipped (invalid format)**: {total_skipped:,}
- **Attempted to fetch**: {total_attempted:,}
- **Successfully fetched**: {total_successful:,}
- **Failed to fetch**: {total_failed:,}
- **Success rate**: {success_rate:.1f}%

### Data Added

- **Total new rows**: {total_rows_added:,}
- **New unique tickers**: {total_successful:,}
- **Date range**: {START_DATE} to {END_DATE}

---

## Successfully Fetched Tickers ({total_successful})

"""

    if stats["successful"]:
        report += "| Original Ticker | Yahoo Ticker | Rows | Date Range |\n"
        report += "|----------------|--------------|------|------------|\n"
        for s in stats["successful"]:
            report += f"| {s['original_ticker']} | {s['yahoo_ticker']} | {s['rows']:,} | {s['date_min']} to {s['date_max']} |\n"
    else:
        report += "*No tickers were successfully fetched.*\n"
    
    report += f"\n---\n\n## Failed Fetches ({total_failed})\n\n"
    
    if stats["failed"]:
        report += "| Ticker | Yahoo Ticker | Error |\n"
        report += "|--------|--------------|-------|\n"
        for f in stats["failed"][:100]:  # Limit to first 100 to keep report manageable
            error = f['error'][:50] + "..." if len(f['error']) > 50 else f['error']
            report += f"| {f['ticker']} | {f['yahoo_ticker']} | {error} |\n"
        
        if total_failed > 100:
            report += f"\n*...and {total_failed - 100} more failed fetches (truncated for brevity)*\n"
    else:
        report += "*All attempted fetches were successful.*\n"
    
    report += f"\n---\n\n## Skipped Tickers ({total_skipped})\n\n"
    report += "These tickers were identified as invalid formats (bonds, CUSIPs, foreign tickers, etc.) and were not queried:\n\n"
    
    if stats["invalid_tickers"]:
        # Group by category for readability
        report += "Examples (first 50):\n\n"
        for ticker in stats["invalid_tickers"][:50]:
            report += f"- {ticker}\n"
        
        if total_skipped > 50:
            report += f"\n*...and {total_skipped - 50} more skipped tickers*\n"
    
    report += f"""

---

## Configuration

- **Start Date**: {START_DATE}
- **End Date**: {END_DATE}
- **Batch Size**: {BATCH_SIZE}
- **Sleep Between Batches**: {SLEEP_BETWEEN_BATCHES}s

## Output Files

- **Enhanced Stock Data**: `{OUTPUT_STOCK_PATH}`
- **Original Filtered Data**: `{FILTERED_STOCK_PATH}`

## Next Steps

1. Review successfully fetched tickers to ensure data quality
2. Re-run congressional trading filter script with enhanced stock data
3. Compare coverage improvement in congressional trades

## Reproducibility

**Script**: `/Users/caleb/Research/congress_trading/fetch_missing_tickers.py`

**Run command**:
```bash
rye run python /Users/caleb/Research/congress_trading/fetch_missing_tickers.py
```

**Note**: Yahoo Finance API has rate limits. Large batches may require multiple runs or longer sleep times.
"""

    with open(report_path, "w") as f:
        f.write(report)
    
    print(f"\nReport saved to: {report_path}")


def main():
    """Main execution function."""
    print("="*70)
    print("Yahoo Finance Missing Ticker Fetch")
    print("="*70)
    print(f"Date range: {START_DATE} to {END_DATE}")
    
    # Step 1: Get missing tickers
    missing_tickers = get_missing_tickers()
    
    # Step 2: Filter to valid tickers
    valid_tickers, invalid_tickers = filter_tickers(missing_tickers)
    
    if not valid_tickers:
        print("\n✗ No valid tickers to fetch. Exiting.")
        return
    
    # Step 3: Fetch data
    print(f"\n{'='*70}")
    print(f"Fetching data for {len(valid_tickers)} tickers from Yahoo Finance")
    print(f"{'='*70}\n")
    
    results = fetch_batch(valid_tickers, START_DATE, END_DATE)
    
    # Save checkpoint of fetched data
    if results["data_frames"]:
        checkpoint_path = "/Users/caleb/Research/congress_trading/data/derived/yahoo_fetch_checkpoint.csv"
        print(f"\nSaving checkpoint of fetched data to {checkpoint_path}...")
        df_checkpoint = pd.concat(results["data_frames"], ignore_index=True)
        df_checkpoint.to_csv(checkpoint_path, index=False)
        print(f"✓ Checkpoint saved ({len(df_checkpoint):,} rows)")
    
    # Step 4: Append to existing stock data
    if results["data_frames"]:
        df_combined = append_to_stock_data(
            results["data_frames"],
            FILTERED_STOCK_PATH,
            OUTPUT_STOCK_PATH
        )
    
    # Step 5: Generate report
    stats = {
        "total_missing": len(missing_tickers),
        "valid_tickers": valid_tickers,
        "invalid_tickers": invalid_tickers,
        "successful": results["successful"],
        "failed": results["failed"]
    }
    
    write_report(stats, REPORT_PATH)
    
    # Final summary
    print("\n" + "="*70)
    print("FETCH COMPLETE")
    print("="*70)
    print(f"\nSuccessfully fetched: {len(results['successful'])} tickers")
    print(f"Failed to fetch: {len(results['failed'])} tickers")
    print(f"Skipped (invalid): {len(invalid_tickers)} tickers")
    
    if results["data_frames"]:
        total_new_rows = sum(len(df) for df in results["data_frames"])
        print(f"Total new rows added: {total_new_rows:,}")
        print(f"\nEnhanced stock data saved to:")
        print(f"  {OUTPUT_STOCK_PATH}")
    
    print(f"\nFetch report saved to:")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()
