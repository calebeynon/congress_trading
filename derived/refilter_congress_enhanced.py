"""
Re-filter Congressional Trading Data with Enhanced Stock Data

Purpose:
    Use the Yahoo Finance-enhanced stock data to recover additional congressional trades
    that were previously removed due to missing tickers.

Author: Generated for congressional trading analysis
Date: 2025-11-09
"""

import pandas as pd
from datetime import datetime

# Global constants
RAW_CONGRESS_PATH = "/Users/caleb/Research/congress_trading/data/raw/congress_trading.csv"
ENHANCED_STOCK_PATH = "/Users/caleb/Research/congress_trading/data/derived/all_stock_data_filtered_enhanced.csv"
OUT_CONGRESS_PATH = "/Users/caleb/Research/congress_trading/data/derived/congress_trading_filtered_enhanced.csv"
REPORT_PATH = "/Users/caleb/Research/congress_trading/data/derived/congress_refilter_report.md"


def standardize_ticker_series(series):
    """Standardize ticker symbols to uppercase and strip whitespace."""
    return series.astype("string").str.upper().str.strip()


def get_member_id_col(df):
    """Return the appropriate member ID column name."""
    if "BioGuideID" in df.columns:
        return "BioGuideID"
    return "Name"


def main():
    """Main execution function."""
    print("="*70)
    print("Re-filtering Congressional Trades with Enhanced Stock Data")
    print("="*70)
    
    # Load enhanced stock data to get available tickers
    print("\nLoading enhanced stock data...")
    df_stock = pd.read_csv(ENHANCED_STOCK_PATH, usecols=["Ticker"], dtype={"Ticker": "string"})
    df_stock["Ticker"] = standardize_ticker_series(df_stock["Ticker"])
    stock_tickers = set(df_stock["Ticker"].dropna().unique())
    print(f"  Available tickers: {len(stock_tickers):,}")
    
    # Load original congressional trading data
    print("\nLoading original congressional trading data...")
    df_congress = pd.read_csv(RAW_CONGRESS_PATH, dtype={"Ticker": "string"})
    
    # Parse dates
    if "Traded" in df_congress.columns:
        df_congress["Traded"] = pd.to_datetime(df_congress["Traded"], errors="coerce")
        date_col = "Traded"
    elif "Filed" in df_congress.columns:
        df_congress["Filed"] = pd.to_datetime(df_congress["Filed"], errors="coerce")
        date_col = "Filed"
    else:
        date_col = None
    
    # Standardize tickers
    df_congress["Ticker"] = standardize_ticker_series(df_congress["Ticker"])
    
    # Get original stats
    member_col = get_member_id_col(df_congress)
    
    original_stats = {
        "rows": len(df_congress),
        "tickers": set(df_congress["Ticker"].dropna().unique()),
        "unique_members": df_congress[member_col].nunique(),
        "date_min": df_congress[date_col].min() if date_col else None,
        "date_max": df_congress[date_col].max() if date_col else None,
    }
    
    print(f"  Total trades: {original_stats['rows']:,}")
    print(f"  Unique tickers: {len(original_stats['tickers']):,}")
    print(f"  Unique members: {original_stats['unique_members']:,}")
    if date_col:
        print(f"  Date range: {original_stats['date_min']} to {original_stats['date_max']}")
    
    # Filter by available tickers
    print("\nFiltering by available tickers...")
    df_filtered = df_congress[df_congress["Ticker"].isin(stock_tickers)].copy()
    
    # Get filtered stats
    filtered_stats = {
        "rows": len(df_filtered),
        "tickers": set(df_filtered["Ticker"].dropna().unique()),
        "unique_members": df_filtered[member_col].nunique(),
        "date_min": df_filtered[date_col].min() if date_col else None,
        "date_max": df_filtered[date_col].max() if date_col else None,
    }
    
    print(f"  Filtered trades: {filtered_stats['rows']:,}")
    print(f"  Unique tickers: {len(filtered_stats['tickers']):,}")
    print(f"  Unique members: {filtered_stats['unique_members']:,}")
    if date_col:
        print(f"  Date range: {filtered_stats['date_min']} to {filtered_stats['date_max']}")
    
    # Calculate changes
    rows_removed = original_stats["rows"] - filtered_stats["rows"]
    pct_removed = (rows_removed / original_stats["rows"]) * 100
    tickers_removed = len(original_stats["tickers"]) - len(filtered_stats["tickers"])
    pct_tickers_removed = (tickers_removed / len(original_stats["tickers"])) * 100
    
    print(f"\nRemoved:")
    print(f"  Trades: {rows_removed:,} ({pct_removed:.2f}%)")
    print(f"  Tickers: {tickers_removed:,} ({pct_tickers_removed:.2f}%)")
    
    # Save filtered data
    print(f"\nSaving filtered congressional data to {OUT_CONGRESS_PATH}...")
    df_filtered.to_csv(OUT_CONGRESS_PATH, index=False)
    
    # Load first-pass filtered data for comparison
    print("\nLoading first-pass filtered data for comparison...")
    df_first_pass = pd.read_csv(
        "/Users/caleb/Research/congress_trading/data/derived/congress_trading_filtered.csv",
        usecols=["Ticker"]
    )
    df_first_pass["Ticker"] = standardize_ticker_series(df_first_pass["Ticker"])
    first_pass_rows = len(df_first_pass)
    first_pass_tickers = df_first_pass["Ticker"].nunique()
    
    # Calculate recovery
    rows_recovered = filtered_stats["rows"] - first_pass_rows
    tickers_recovered = len(filtered_stats["tickers"]) - first_pass_tickers
    pct_recovery = (rows_recovered / rows_removed) * 100 if rows_removed > 0 else 0
    
    print(f"\nComparison with first-pass filtering:")
    print(f"  First pass: {first_pass_rows:,} trades, {first_pass_tickers:,} tickers")
    print(f"  Enhanced: {filtered_stats['rows']:,} trades, {len(filtered_stats['tickers']):,} tickers")
    print(f"  Recovered: {rows_recovered:,} trades ({pct_recovery:.2f}% of removed)")
    print(f"  Tickers recovered: {tickers_recovered:,}")
    
    # Generate report
    write_report(original_stats, filtered_stats, first_pass_rows, first_pass_tickers,
                rows_removed, pct_removed, tickers_removed, pct_tickers_removed,
                rows_recovered, tickers_recovered, member_col, date_col)
    
    print("\n" + "="*70)
    print("FILTERING COMPLETE")
    print("="*70)
    print(f"\nOutput file: {OUT_CONGRESS_PATH}")
    print(f"Report: {REPORT_PATH}")


def write_report(original, filtered, first_pass_rows, first_pass_tickers,
                rows_removed, pct_removed, tickers_removed, pct_tickers_removed,
                rows_recovered, tickers_recovered, member_col, date_col):
    """Generate markdown report of refiltering results."""
    
    report = f"""# Congressional Trading Re-filtering Report (Enhanced Data)

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Purpose

This report documents the re-filtering of congressional trading data using the Yahoo Finance-enhanced stock dataset. By adding 1,057 tickers via Yahoo Finance, we can now match additional congressional trades that were previously removed.

## Data Sources

- **Congressional Trading**: `data/raw/congress_trading.csv`
- **Enhanced Stock Data**: `data/derived/all_stock_data_filtered_enhanced.csv` (10,372 tickers)

## Results

### Original Congressional Data
- **Total trades**: {original['rows']:,}
- **Unique tickers**: {len(original['tickers']):,}
- **Unique members**: {original['unique_members']:,}
"""
    
    if date_col:
        report += f"- **Date range**: {original['date_min']} to {original['date_max']}\n"
    
    report += f"""
### Enhanced Filtered Data
- **Total trades**: {filtered['rows']:,}
- **Unique tickers**: {len(filtered['tickers']):,}
- **Unique members**: {filtered['unique_members']:,}
"""
    
    if date_col:
        report += f"- **Date range**: {filtered['date_min']} to {filtered['date_max']}\n"
    
    report += f"""
### Removed (Still Unmatched)
- **Trades**: {rows_removed:,} ({pct_removed:.2f}%)
- **Tickers**: {tickers_removed:,} ({pct_tickers_removed:.2f}%)

---

## Improvement Over First-Pass Filtering

The first-pass filtering used only the original stock data (9,315 tickers). With Yahoo Finance enhancement (10,372 tickers), we recovered additional trades:

| Metric | First Pass | Enhanced | Recovered |
|--------|-----------|----------|-----------|
| **Trades** | {first_pass_rows:,} | {filtered['rows']:,} | **{rows_recovered:,}** |
| **Tickers** | {first_pass_tickers:,} | {len(filtered['tickers']):,} | **{tickers_recovered:,}** |

### Trade Recovery Rate
- **{rows_recovered:,} additional trades** recovered ({(rows_recovered/original['rows']*100):.2f}% of original dataset)
- This represents **{(rows_recovered/(original['rows']-first_pass_rows)*100):.2f}%** of trades that were removed in first pass

### Ticker Coverage
- **Original coverage**: {(first_pass_tickers/len(original['tickers'])*100):.1f}% ({first_pass_tickers:,}/{len(original['tickers']):,})
- **Enhanced coverage**: {(len(filtered['tickers'])/len(original['tickers'])*100):.1f}% ({len(filtered['tickers']):,}/{len(original['tickers']):,})
- **Improvement**: +{((len(filtered['tickers'])-first_pass_tickers)/len(original['tickers'])*100):.1f} percentage points

---

## Final Dataset Summary

### Coverage Statistics
- **Trade retention rate**: {(filtered['rows']/original['rows']*100):.2f}%
- **Ticker retention rate**: {(len(filtered['tickers'])/len(original['tickers'])*100):.2f}%
- **Member retention rate**: {(filtered['unique_members']/original['unique_members']*100):.2f}%

### Unmatched Trades
{rows_removed:,} trades ({pct_removed:.2f}%) remain unmatched. These likely involve:
- Foreign securities (non-US exchanges)
- Fixed income instruments (bonds, treasuries)
- Options and derivatives
- Data entry errors
- Delisted companies not in Yahoo Finance

---

## Output File

**File**: `data/derived/congress_trading_filtered_enhanced.csv`

**Columns**: Same as original congressional trading data  
**Member ID**: `{member_col}`  
**Date Column**: `{date_col if date_col else 'N/A'}`

---

## Reproducibility

**Script**: `derived/refilter_congress_enhanced.py`

**Run command**:
```bash
rye run python derived/refilter_congress_enhanced.py
```

**Dependencies**:
- Enhanced stock data must exist at: `data/derived/all_stock_data_filtered_enhanced.csv`
- Original congressional data at: `data/raw/congress_trading.csv`

---

## Next Steps

This enhanced dataset is now ready for difference-in-differences analysis:

1. **Sentiment Spike Identification**: Define events from news sentiment data
2. **Volume Analysis**: Compare congressional vs non-congressional stock volume
3. **Pre-spike Window Analysis**: Test for anticipatory trading (t-3, t-5, t-7 days)
4. **Heterogeneity Analysis**: By party, committee, seniority, trade type

**Final Sample Size**: {filtered['rows']:,} congressional trades across {len(filtered['tickers']):,} tickers (2012-2024)
"""

    with open(REPORT_PATH, "w") as f:
        f.write(report)
    
    print(f"\nReport saved to: {REPORT_PATH}")


if __name__ == "__main__":
    main()
