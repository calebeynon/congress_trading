# Congressional Trading & Market Volume Aggregation Scripts

Quick reference guide for `cong_agg_date.py` and `market_volume_agg_date.py`.

## Overview

Two scripts that aggregate trading data around a specific date with a ±N day window:

1. **`cong_agg_date.py`** - Congressional trading aggregated by date with ticker weights
2. **`market_volume_agg_date.py`** - Major market index volumes for comparison

Both use the same date window approach and can be combined for analysis.

---

## 1. Congressional Trading Aggregation (`cong_agg_date.py`)

### What It Does
- Aggregates congressional trades ±30 days (default) around a date
- Calculates total daily dollar volume across all trades
- Computes each ticker's percentage of daily volume (0-100 scale)
- Returns wide-format DataFrame with one column per ticker

### Input Data
- Source: `data/derived/congress_trading_filtered_enhanced.csv`
- Uses: `Traded`, `Ticker`, `Trade_Size_USD`, `Transaction`

### Output Format
```
Date        | Total_Trade_Size_USD | AAPL  | GOOGL | MSFT | ...
------------|----------------------|-------|-------|------|----
2025-07-27  |          150000.0    | 15.5  |  20.3 | 10.2 | ...
2025-07-28  |          389005.5    | 12.1  |   8.7 | 15.3 | ...
```

### Usage

**Python Import:**
```python
from derived.cong_agg_date import get_aggregated_window

# Basic usage - 30 day window
df = get_aggregated_window("2024-08-26")

# Custom window
df = get_aggregated_window("2024-01-15", window_days=45)

# With export
from derived.cong_agg_date import main
df = main("2024-08-26", output_path="output/congress_trades.csv")
```

**Command Line:**
```bash
# Basic
python derived/cong_agg_date.py --date 2024-08-26

# With custom window and export
python derived/cong_agg_date.py --date 2024-08-26 --window 45 --output results.csv
```

### Key Features
- **Dollar Volume**: Parses "$1,001 - $15,000" format to midpoint (8000.5)
- **Both Buy/Sell**: Treats Purchases and Sales as positive contributions
- **Ticker Weights**: Each ticker's % of daily total (weights sum to 100% per day)
- **Complete Coverage**: Every day in window included (zeros if no trades)

---

## 2. Market Index Volume Aggregation (`market_volume_agg_date.py`)

### What It Does
- Extracts volume for 6 major market indices around a date
- Provides baseline comparison for congressional trading activity
- Same date window approach as congressional script

### Tracked Indices
- **SPY** - S&P 500 ETF
- **DIA** - Dow Jones Industrial Average ETF
- **QQQ** - NASDAQ-100 ETF
- **IWM** - Russell 2000 ETF
- **VOO** - Vanguard S&P 500 ETF
- **VTI** - Vanguard Total Stock Market ETF

### Input Data
- Source: `data/derived/all_stock_data_filtered_enhanced.csv`
- Uses: `Date`, `Ticker`, `Volume`

### Output Format
```
Date        | Dow_Jones_Volume | NASDAQ_100_Volume | S&P_500_Volume | ...
------------|------------------|-------------------|----------------|----
2024-07-27  |         5234000  |         45123000  |       78456000 | ...
2024-07-28  |         4891000  |         42567000  |       73210000 | ...
```

### Usage

**Python Import:**
```python
from derived.market_volume_agg_date import get_market_volumes

# Basic usage
df = get_market_volumes("2024-08-26")

# Custom window
df = get_market_volumes("2024-01-15", window_days=45)

# With export
from derived.market_volume_agg_date import main
df = main("2024-08-26", output_path="output/market_volumes.csv")
```

**Command Line:**
```bash
# Basic
python derived/market_volume_agg_date.py --date 2024-08-26

# With custom window and export
python derived/market_volume_agg_date.py --date 2024-08-26 --window 45 --output results.csv
```

---

## 3. Combined Usage Example

```python
from derived.cong_agg_date import get_aggregated_window
from derived.market_volume_agg_date import get_market_volumes
import pandas as pd

# Get both datasets for the same window
date = "2024-08-26"
window = 30

cong_df = get_aggregated_window(date, window_days=window)
market_df = get_market_volumes(date, window_days=window)

# Merge on date for comparison
combined = cong_df.merge(market_df, on="Date", how="inner")

# Example analysis: Compare congressional volume to S&P 500 volume
combined["Congress_vs_SPY_Ratio"] = (
    combined["Total_Trade_Size_USD"] / combined["S&P_500_Volume"]
)

print(combined[["Date", "Total_Trade_Size_USD", "S&P_500_Volume", "Congress_vs_SPY_Ratio"]].head())
```

---

## 4. Common Parameters

Both scripts share these parameters:

| Parameter     | Type              | Default | Description                           |
|---------------|-------------------|---------|---------------------------------------|
| `date_input`  | str or Timestamp  | -       | Center date (e.g., "2024-08-26")     |
| `window_days` | int              | 30      | Days before and after center date     |
| `output_path` | str or None      | None    | Optional CSV export path              |
| `return_df`   | bool             | True    | Whether to return DataFrame           |

**Date Formats Accepted:**
- `"2024-08-26"` (ISO format)
- `"08/26/2024"` (US format)
- `pd.Timestamp("2024-08-26")`
- `datetime` objects

---

## 5. Notes

### Date Windows
- A window of 30 means ±30 days → 61 total days (including center date)
- Missing dates are filled with zeros
- Dates normalized to midnight (no time component)

### Data Coverage
- **Congressional data**: Check `congress_trading_filtered_enhanced.csv` for date range
- **Market indices**: Check `all_stock_data_filtered_enhanced.csv` for date range
- Using dates outside available data returns zeros

### Performance
- Congressional script processes ~100K trades efficiently
- Market script only loads 6 tickers (very fast)
- Both scripts use `low_memory=False` for large files

### Output Files
- **Do NOT commit CSV outputs to git** (per project rules)
- Keep outputs in `output/` directory
- Files are ignored by `.gitignore` if >1MB

---

## 6. Troubleshooting

**No data returned:**
- Check if date is within data range
- Verify ticker symbols exist in source data
- For market indices: Ensure SPY, DIA, QQQ, etc. are in stock data

**Import errors:**
```python
# Make sure you're in the project root
import sys
sys.path.append("/Users/caleb/Research/congress_trading")
from derived.cong_agg_date import get_aggregated_window
```

**Large memory usage:**
- Both scripts load only necessary columns
- If issues persist, reduce window size
- Consider processing in batches for analysis

---

## 7. Quick Start

```bash
# Test both scripts
cd /Users/caleb/Research/congress_trading

# Congressional trades
rye run python derived/cong_agg_date.py --date 2024-01-15 --window 10

# Market volumes  
rye run python derived/market_volume_agg_date.py --date 2024-01-15 --window 10
```

```python
# Quick Python test
from derived.cong_agg_date import get_aggregated_window
from derived.market_volume_agg_date import get_market_volumes

cong = get_aggregated_window("2024-01-15", window_days=10)
market = get_market_volumes("2024-01-15", window_days=10)

print(f"Congressional trades: {cong.shape}")
print(f"Market indices: {market.shape}")
```
