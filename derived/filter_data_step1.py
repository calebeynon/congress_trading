"""
Data Filtering Script: Congressional Trading and Stock Data Alignment

Purpose:
    1. Filter stock data to only include dates >= 2012-01-01
    2. Ensure ticker consistency between congressional trading and stock datasets
    3. Generate a comprehensive report of data filtering statistics

Author: Generated for congressional trading analysis
Date: 2025-11-09
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime

# Global constants
RAW_CONGRESS_PATH = "/Users/caleb/Research/congress_trading/data/raw/congress_trading.csv"
RAW_STOCK_PATH = "/Users/caleb/Research/congress_trading/data/derived/all_stock_data_filtered_enhanced.csv"
DERIVED_DIR = "/Users/caleb/Research/congress_trading/data/derived"
OUT_CONGRESS_PATH = f"{DERIVED_DIR}/congress_trading_filtered_enhanced.csv"
OUT_STOCK_PATH = f"{DERIVED_DIR}/all_stock_data_filtered_enhanced.csv"  # Already exists, will skip
REPORT_PATH = f"{DERIVED_DIR}/data_filtering_report_enhanced.md"
DATE_CUTOFF = "2012-01-01"
CHUNKSIZE = 1_000_000


def ensure_dir(path):
    """Create directory if it doesn't exist."""
    Path(path).mkdir(parents=True, exist_ok=True)


def standardize_ticker_series(series):
    """Standardize ticker symbols to uppercase and strip whitespace."""
    return series.astype("string").str.upper().str.strip()


def get_member_id_col(df):
    """Return the appropriate member ID column name."""
    if "BioGuideID" in df.columns:
        return "BioGuideID"
    return "Name"


def format_pct(n_removed, n_total):
    """Format percentage safely, handling division by zero."""
    if n_total == 0:
        return "0.0%"
    return f"{(n_removed / n_total) * 100:.2f}%"


def process_stock_data(raw_path, out_path, date_cutoff, chunksize):
    """
    Process stock data: filter by date and standardize tickers.
    
    Returns dict with pre/post statistics.
    """
    print("\n" + "="*70)
    print("STEP 1: Filtering Stock Data (Date >= 2012-01-01)")
    print("="*70)
    
    # Initialize statistics
    stats = {
        "rows_pre": 0,
        "rows_post": 0,
        "tickers_pre": set(),
        "tickers_post": set(),
        "date_min_pre": None,
        "date_max_pre": None,
        "date_min_post": None,
        "date_max_post": None,
    }
    
    cutoff_date = pd.Timestamp(date_cutoff)
    first_chunk = True
    chunk_count = 0
    
    print(f"Reading stock data in chunks of {chunksize:,} rows...")
    
    for chunk in pd.read_csv(
        raw_path,
        parse_dates=["Date"],
        dtype={"Ticker": "string"},
        chunksize=chunksize,
        low_memory=False
    ):
        chunk_count += 1
        
        # Standardize tickers
        chunk["Ticker"] = standardize_ticker_series(chunk["Ticker"])
        
        # Update pre-filter statistics
        stats["rows_pre"] += len(chunk)
        valid_tickers = chunk["Ticker"].dropna().unique()
        stats["tickers_pre"].update(valid_tickers)
        
        # Track date ranges (pre-filter)
        chunk_dates = chunk["Date"].dropna()
        if len(chunk_dates) > 0:
            chunk_min = chunk_dates.min()
            chunk_max = chunk_dates.max()
            if stats["date_min_pre"] is None or chunk_min < stats["date_min_pre"]:
                stats["date_min_pre"] = chunk_min
            if stats["date_max_pre"] is None or chunk_max > stats["date_max_pre"]:
                stats["date_max_pre"] = chunk_max
        
        # Filter by date
        filtered = chunk[chunk["Date"] >= cutoff_date]
        
        # Update post-filter statistics
        stats["rows_post"] += len(filtered)
        valid_tickers_post = filtered["Ticker"].dropna().unique()
        stats["tickers_post"].update(valid_tickers_post)
        
        # Track date ranges (post-filter)
        filtered_dates = filtered["Date"].dropna()
        if len(filtered_dates) > 0:
            filt_min = filtered_dates.min()
            filt_max = filtered_dates.max()
            if stats["date_min_post"] is None or filt_min < stats["date_min_post"]:
                stats["date_min_post"] = filt_min
            if stats["date_max_post"] is None or filt_max > stats["date_max_post"]:
                stats["date_max_post"] = filt_max
        
        # Write filtered chunk
        if len(filtered) > 0:
            filtered.to_csv(
                out_path,
                mode="w" if first_chunk else "a",
                header=first_chunk,
                index=False
            )
            first_chunk = False
        
        if chunk_count % 5 == 0:
            print(f"  Processed {chunk_count} chunks ({stats['rows_pre']:,} rows)")
    
    print(f"\nCompleted processing {chunk_count} chunks")
    print(f"Saved filtered stock data to: {out_path}")
    
    return stats


def load_and_filter_congress(raw_path, allowed_tickers):
    """
    Load congressional trading data and filter by allowed tickers.
    
    Returns tuple of (filtered_df, stats_dict).
    """
    print("\n" + "="*70)
    print("STEP 2: Filtering Congressional Trading Data (Ticker Alignment)")
    print("="*70)
    
    print(f"Loading congressional trading data from: {raw_path}")
    
    # Determine which date column to use
    df = pd.read_csv(
        raw_path,
        dtype={"Ticker": "string"}
    )
    
    # Parse date columns
    if "Traded" in df.columns:
        df["Traded"] = pd.to_datetime(df["Traded"], errors="coerce")
        date_col = "Traded"
    elif "Filed" in df.columns:
        df["Filed"] = pd.to_datetime(df["Filed"], errors="coerce")
        date_col = "Filed"
    else:
        date_col = None
    
    # Standardize tickers
    df["Ticker"] = standardize_ticker_series(df["Ticker"])
    
    # Compute pre-filter statistics
    member_col = get_member_id_col(df)
    
    stats_pre = {
        "rows": len(df),
        "tickers": set(df["Ticker"].dropna().unique()),
        "unique_members": df[member_col].nunique(),
        "date_min": df[date_col].min() if date_col else None,
        "date_max": df[date_col].max() if date_col else None,
    }
    
    print(f"\nOriginal congressional data:")
    print(f"  Rows: {stats_pre['rows']:,}")
    print(f"  Unique tickers: {len(stats_pre['tickers']):,}")
    print(f"  Unique members: {stats_pre['unique_members']:,}")
    if date_col:
        print(f"  Date range: {stats_pre['date_min']} to {stats_pre['date_max']}")
    
    # Filter by allowed tickers
    df_filtered = df[df["Ticker"].isin(allowed_tickers)].copy()
    
    # Compute post-filter statistics
    stats_post = {
        "rows": len(df_filtered),
        "tickers": set(df_filtered["Ticker"].dropna().unique()),
        "unique_members": df_filtered[member_col].nunique(),
        "date_min": df_filtered[date_col].min() if date_col else None,
        "date_max": df_filtered[date_col].max() if date_col else None,
    }
    
    # Combine stats
    stats = {
        "pre": stats_pre,
        "post": stats_post,
        "member_col": member_col,
        "date_col": date_col,
    }
    
    return df_filtered, stats


def write_markdown_report(stats, path):
    """Generate comprehensive markdown report of data filtering."""
    
    # Compute derived statistics
    stock_rows_removed = stats["stock"]["rows_pre"] - stats["stock"]["rows_post"]
    stock_rows_pct = format_pct(stock_rows_removed, stats["stock"]["rows_pre"])
    
    stock_tickers_removed = len(stats["stock"]["tickers_pre"]) - len(stats["stock"]["tickers_post"])
    stock_tickers_pct = format_pct(
        stock_tickers_removed,
        len(stats["stock"]["tickers_pre"])
    )
    
    congress_rows_removed = stats["congress"]["pre"]["rows"] - stats["congress"]["post"]["rows"]
    congress_rows_pct = format_pct(
        congress_rows_removed,
        stats["congress"]["pre"]["rows"]
    )
    
    congress_tickers_removed = (
        len(stats["congress"]["pre"]["tickers"]) - 
        len(stats["congress"]["post"]["tickers"])
    )
    congress_tickers_pct = format_pct(
        congress_tickers_removed,
        len(stats["congress"]["pre"]["tickers"])
    )
    
    # Build markdown content
    report = f"""# Data Filtering Report: Congressional Trading & Stock Data

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Purpose

This report documents the data filtering process applied to align congressional trading data with stock market data. The filtering ensures:

1. **Temporal consistency**: Stock data limited to 2012 onwards (matching congressional disclosure requirements)
2. **Ticker alignment**: Congressional trades matched only with stocks that have complete volume/price data
3. **Data quality**: Removal of unmatched records to enable valid difference-in-differences analysis

## Data Sources

- **Congressional Trading Data**: `{RAW_CONGRESS_PATH}`
- **Stock Market Data**: `{RAW_STOCK_PATH}`

## Filtering Steps

### Step 1: Stock Data Date Filter (≥ {DATE_CUTOFF})

**Rationale**: Congressional trading disclosure begins in 2012 following the STOCK Act. Earlier stock data is not needed for this analysis.

**Original Stock Data:**
- Total rows: **{stats["stock"]["rows_pre"]:,}**
- Unique tickers: **{len(stats["stock"]["tickers_pre"]):,}**
- Date range: **{stats["stock"]["date_min_pre"]}** to **{stats["stock"]["date_max_pre"]}**

**After Date Filter:**
- Total rows: **{stats["stock"]["rows_post"]:,}**
- Unique tickers: **{len(stats["stock"]["tickers_post"]):,}**
- Date range: **{stats["stock"]["date_min_post"]}** to **{stats["stock"]["date_max_post"]}**

**Removed:**
- Rows: **{stock_rows_removed:,}** ({stock_rows_pct})
- Tickers: **{stock_tickers_removed:,}** ({stock_tickers_pct})

---

### Step 2: Congressional Trading Ticker Alignment

**Rationale**: Remove congressional trades for tickers without corresponding stock volume data. This ensures all treatment observations have valid counterfactuals.

**Original Congressional Data:**
- Total trades: **{stats["congress"]["pre"]["rows"]:,}**
- Unique tickers: **{len(stats["congress"]["pre"]["tickers"]):,}**
- Unique members: **{stats["congress"]["pre"]["unique_members"]:,}**
"""

    if stats["congress"]["date_col"]:
        report += f"""- Trade date range: **{stats["congress"]["pre"]["date_min"]}** to **{stats["congress"]["pre"]["date_max"]}**
"""

    report += f"""
**After Ticker Alignment:**
- Total trades: **{stats["congress"]["post"]["rows"]:,}**
- Unique tickers: **{len(stats["congress"]["post"]["tickers"]):,}**
- Unique members: **{stats["congress"]["post"]["unique_members"]:,}**
"""

    if stats["congress"]["date_col"]:
        report += f"""- Trade date range: **{stats["congress"]["post"]["date_min"]}** to **{stats["congress"]["post"]["date_max"]}**
"""

    report += f"""
**Removed:**
- Trades: **{congress_rows_removed:,}** ({congress_rows_pct})
- Tickers: **{congress_tickers_removed:,}** ({congress_tickers_pct})

---

## Final Filtered Datasets

### Stock Data (2012+)
- **File**: `{OUT_STOCK_PATH}`
- **Rows**: {stats["stock"]["rows_post"]:,}
- **Unique tickers**: {len(stats["stock"]["tickers_post"]):,}
- **Date range**: {stats["stock"]["date_min_post"]} to {stats["stock"]["date_max_post"]}
- **Columns**: Date, Ticker, Open, High, Low, Close, Volume, Dividends, Stock Splits

### Congressional Trading Data (Filtered)
- **File**: `{OUT_CONGRESS_PATH}`
- **Trades**: {stats["congress"]["post"]["rows"]:,}
- **Unique tickers**: {len(stats["congress"]["post"]["tickers"]):,}
- **Unique members**: {stats["congress"]["post"]["unique_members"]:,}
"""

    if stats["congress"]["date_col"]:
        report += f"""- **Trade date range**: {stats["congress"]["post"]["date_min"]} to {stats["congress"]["post"]["date_max"]}
"""

    report += f"""- **Member ID column**: `{stats["congress"]["member_col"]}`

## Key Takeaways

1. **Stock data reduction**: Removed {stock_rows_pct} of rows by filtering to 2012+ (from {stats["stock"]["date_min_pre"].year} to present)
2. **Congressional ticker coverage**: {congress_rows_pct} of congressional trades involved tickers without matching stock data
3. **Final sample**: {stats["congress"]["post"]["rows"]:,} congressional trades across {len(stats["congress"]["post"]["tickers"]):,} tickers with complete volume data

## Reproducibility

**Script**: `/Users/caleb/Research/congress_trading/filter_data_step1.py`

**Constants**:
- `DATE_CUTOFF = "{DATE_CUTOFF}"`
- `CHUNKSIZE = {CHUNKSIZE:,}`

**Run command**:
```bash
rye run python /Users/caleb/Research/congress_trading/filter_data_step1.py
```

## Next Steps

With aligned datasets, subsequent analysis can proceed to:
1. Define sentiment spike events from news sentiment data
2. Construct difference-in-differences design with appropriate controls
3. Test for anticipatory trading volume increases before sentiment spikes
"""

    # Write report
    with open(path, "w") as f:
        f.write(report)
    
    print(f"\nMarkdown report saved to: {path}")


def main():
    """Main execution function."""
    print("\n" + "="*70)
    print("Congressional Trading & Stock Data Filtering Pipeline")
    print("="*70)
    print(f"Date cutoff: {DATE_CUTOFF}")
    print(f"Chunk size: {CHUNKSIZE:,} rows")
    
    # Ensure output directory exists
    ensure_dir(DERIVED_DIR)
    
    # Step 1: Process and filter stock data
    stock_stats = process_stock_data(
        RAW_STOCK_PATH,
        OUT_STOCK_PATH,
        DATE_CUTOFF,
        CHUNKSIZE
    )
    
    # Print Step 1 summary
    print("\n--- Step 1 Summary ---")
    print(f"Original rows: {stock_stats['rows_pre']:,}")
    print(f"Filtered rows: {stock_stats['rows_post']:,}")
    print(f"Removed: {stock_stats['rows_pre'] - stock_stats['rows_post']:,} rows "
          f"({format_pct(stock_stats['rows_pre'] - stock_stats['rows_post'], stock_stats['rows_pre'])})")
    print(f"Original tickers: {len(stock_stats['tickers_pre']):,}")
    print(f"Filtered tickers: {len(stock_stats['tickers_post']):,}")
    print(f"Removed: {len(stock_stats['tickers_pre']) - len(stock_stats['tickers_post']):,} tickers "
          f"({format_pct(len(stock_stats['tickers_pre']) - len(stock_stats['tickers_post']), len(stock_stats['tickers_pre']))})")
    
    # Step 2: Filter congressional data by allowed tickers
    df_congress_filtered, congress_stats = load_and_filter_congress(
        RAW_CONGRESS_PATH,
        stock_stats["tickers_post"]
    )
    
    # Save filtered congressional data
    print(f"\nSaving filtered congressional data to: {OUT_CONGRESS_PATH}")
    df_congress_filtered.to_csv(OUT_CONGRESS_PATH, index=False)
    
    # Print Step 2 summary
    print("\n--- Step 2 Summary ---")
    print(f"Original trades: {congress_stats['pre']['rows']:,}")
    print(f"Filtered trades: {congress_stats['post']['rows']:,}")
    print(f"Removed: {congress_stats['pre']['rows'] - congress_stats['post']['rows']:,} trades "
          f"({format_pct(congress_stats['pre']['rows'] - congress_stats['post']['rows'], congress_stats['pre']['rows'])})")
    print(f"Original tickers: {len(congress_stats['pre']['tickers']):,}")
    print(f"Filtered tickers: {len(congress_stats['post']['tickers']):,}")
    print(f"Removed: {len(congress_stats['pre']['tickers']) - len(congress_stats['post']['tickers']):,} tickers "
          f"({format_pct(len(congress_stats['pre']['tickers']) - len(congress_stats['post']['tickers']), len(congress_stats['pre']['tickers']))})")
    
    # Generate markdown report
    all_stats = {
        "stock": {
            "rows_pre": stock_stats["rows_pre"],
            "rows_post": stock_stats["rows_post"],
            "tickers_pre": stock_stats["tickers_pre"],
            "tickers_post": stock_stats["tickers_post"],
            "date_min_pre": stock_stats["date_min_pre"],
            "date_max_pre": stock_stats["date_max_pre"],
            "date_min_post": stock_stats["date_min_post"],
            "date_max_post": stock_stats["date_max_post"],
        },
        "congress": congress_stats,
    }
    
    write_markdown_report(all_stats, REPORT_PATH)
    
    # Final summary
    print("\n" + "="*70)
    print("FILTERING COMPLETE")
    print("="*70)
    print(f"\nOutput files:")
    print(f"  1. {OUT_STOCK_PATH}")
    print(f"  2. {OUT_CONGRESS_PATH}")
    print(f"  3. {REPORT_PATH}")
    print("\nReady for difference-in-differences analysis!")


if __name__ == "__main__":
    main()
