"""
Congressional Trading Aggregation by Date Window

Purpose:
    Aggregate congressional stock trading data around a specified date, creating a
    wide-format dataset with daily totals and ticker-specific percentage weights.

Methodology:
    - Takes an input date and creates a window of ±N days (default 30)
    - Parses Trade_Size_USD ranges (e.g., "$1,001 - $15,000") to midpoint values
    - Aggregates both Purchases and Sales as positive contributions
    - Computes each ticker's percentage share of daily trading volume (0-100 scale)
    - Returns wide-format DataFrame with one column per ticker

Assumptions:
    - Input data contains columns: Traded (or Date), Ticker, Trade_Size_USD, Transaction
    - Trade_Size_USD format: "$low - $high" or "$value"
    - Missing or malformed data is handled gracefully with warnings

Example Usage:
    # Import usage
    from derived.cong_agg_date import get_aggregated_window
    df = get_aggregated_window("2025-08-26")
    df = get_aggregated_window("2024-01-15", window_days=45)
    
    # CLI usage
    python derived/cong_agg_date.py --date 2025-08-26 --window 30 --output output.csv

Author: Generated for congressional trading analysis
Date: 2025-11-12
"""

import re
import warnings
from pathlib import Path
from typing import Union, Optional, Tuple
import argparse

import numpy as np
import pandas as pd


# Global constants
DATA_PATH = Path("/Users/caleb/Research/congress_trading/data/derived/congress_trading_filtered_enhanced.csv")
DEFAULT_DATE_WINDOW_DAYS = 30
DATE_COLUMN_CANDIDATES = ("Traded", "Date", "date")
TICKER_COL = "Ticker"
TRADE_SIZE_COL = "Trade_Size_USD"
TRANSACTION_COL = "Transaction"
OUTPUT_TOTAL_COL = "Total_Trade_Size_USD"
OUTPUT_DATE_COL = "Date"
PERCENT_SCALE = 100.0


def parse_date_input(date_input: Union[str, pd.Timestamp, pd.DatetimeIndex]) -> pd.Timestamp:
    """
    Parse flexible date input to normalized pandas Timestamp.
    
    Args:
        date_input: Date as string (e.g., "2025-08-26", "08/26/2025"), 
                   datetime object, or pandas Timestamp
    
    Returns:
        Normalized pandas Timestamp (time set to midnight)
    
    Raises:
        ValueError: If date cannot be parsed
    
    Examples:
        >>> parse_date_input("2025-08-26")
        Timestamp('2025-08-26 00:00:00')
        >>> parse_date_input(pd.Timestamp("2024-01-15"))
        Timestamp('2024-01-15 00:00:00')
    """
    try:
        parsed_date = pd.to_datetime(date_input, errors="coerce")
        
        if pd.isna(parsed_date):
            raise ValueError(
                f"Could not parse date input: {date_input}. "
                f"Try formats like 'YYYY-MM-DD', 'MM/DD/YYYY', or datetime objects."
            )
        
        return parsed_date.normalize()
    
    except Exception as e:
        raise ValueError(f"Error parsing date input '{date_input}': {str(e)}")


def load_congress_trades(data_path: Path = DATA_PATH) -> Tuple[pd.DataFrame, str]:
    """
    Load and standardize congressional trading data.
    
    Args:
        data_path: Path to congressional trading CSV file
    
    Returns:
        Tuple of (DataFrame, date_column_name)
    
    Raises:
        KeyError: If required columns are missing
        FileNotFoundError: If data file doesn't exist
    """
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")
    
    df = pd.read_csv(data_path, low_memory=False)
    
    # Identify date column
    date_col = None
    for candidate in DATE_COLUMN_CANDIDATES:
        if candidate in df.columns:
            date_col = candidate
            break
    
    if date_col is None:
        raise KeyError(
            f"No date column found. Expected one of: {DATE_COLUMN_CANDIDATES}. "
            f"Found columns: {list(df.columns)}"
        )
    
    # Parse and clean date column
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    initial_rows = len(df)
    df = df.dropna(subset=[date_col])
    dropped_dates = initial_rows - len(df)
    
    if dropped_dates > 0:
        warnings.warn(f"Dropped {dropped_dates} rows with invalid dates")
    
    # Check for required columns
    if TICKER_COL not in df.columns:
        raise KeyError(f"Required column '{TICKER_COL}' not found in data")
    
    if TRADE_SIZE_COL not in df.columns:
        raise KeyError(f"Required column '{TRADE_SIZE_COL}' not found in data")
    
    # Handle missing tickers
    initial_rows = len(df)
    df = df.dropna(subset=[TICKER_COL])
    dropped_tickers = initial_rows - len(df)
    
    if dropped_tickers > 0:
        warnings.warn(f"Dropped {dropped_tickers} rows with missing tickers")
    
    # Ensure Transaction column exists (optional, but helpful)
    if TRANSACTION_COL not in df.columns:
        df[TRANSACTION_COL] = "Unknown"
    
    return df, date_col


def parse_trade_size_to_mid(series: pd.Series) -> pd.Series:
    """
    Parse Trade_Size_USD column to midpoint numeric values.
    
    Handles formats like:
        - "$1,001 - $15,000" -> 8000.5
        - "$50,000" -> 50000.0
        - "Unknown" -> NaN
    
    Args:
        series: pandas Series containing trade size strings
    
    Returns:
        pandas Series of float midpoint values
    
    Examples:
        >>> parse_trade_size_to_mid(pd.Series(["$1,001 - $15,000", "$50,000"]))
        0     8000.5
        1    50000.0
        dtype: float64
    """
    def extract_midpoint(value):
        if pd.isna(value):
            return np.nan
        
        # Convert to string and extract all numbers
        value_str = str(value)
        numbers = re.findall(r"[0-9][0-9,]*\.?[0-9]*", value_str)
        
        if len(numbers) == 0:
            return np.nan
        
        # Remove commas and convert to float
        clean_numbers = [float(n.replace(",", "")) for n in numbers]
        
        if len(clean_numbers) >= 2:
            # Take first two as low and high, compute midpoint
            return (clean_numbers[0] + clean_numbers[1]) / 2.0
        
        # Single number - use as-is
        return clean_numbers[0]
    
    return series.apply(extract_midpoint)


def filter_date_window(
    df: pd.DataFrame, 
    date_col: str, 
    anchor_date: pd.Timestamp, 
    window_days: int
) -> Tuple[pd.DataFrame, pd.DatetimeIndex]:
    """
    Filter DataFrame to ±window_days around anchor date.
    
    Args:
        df: DataFrame with date column
        date_col: Name of date column
        anchor_date: Center date for window
        window_days: Number of days before and after anchor date
    
    Returns:
        Tuple of (filtered DataFrame, complete date range index)
    """
    start_date = anchor_date - pd.Timedelta(days=window_days)
    end_date = anchor_date + pd.Timedelta(days=window_days)
    
    # Filter to window
    df_window = df[
        (df[date_col] >= start_date) & 
        (df[date_col] <= end_date)
    ].copy()
    
    # Create complete date range for full window
    full_index = pd.date_range(
        start_date.normalize(), 
        end_date.normalize(), 
        freq="D"
    )
    
    return df_window, full_index


def aggregate_by_date_ticker(
    df_window: pd.DataFrame, 
    date_col: str, 
    ticker_col: str, 
    size_mid_col: str
) -> pd.DataFrame:
    """
    Aggregate trades by date and ticker, computing percentage weights.
    
    Both Purchases and Sales are treated as positive contributions to volume.
    
    Args:
        df_window: Filtered DataFrame with trades in window
        date_col: Name of date column
        ticker_col: Name of ticker column
        size_mid_col: Name of column with midpoint trade sizes
    
    Returns:
        Wide DataFrame with Date, Total_Trade_Size_USD, and ticker weight columns
    """
    if len(df_window) == 0:
        # Return empty DataFrame with correct structure
        return pd.DataFrame(columns=[OUTPUT_DATE_COL, OUTPUT_TOTAL_COL])
    
    # Normalize dates to midnight
    df_window[date_col] = df_window[date_col].dt.normalize()
    
    # Group by date and ticker, sum trade sizes
    grouped = df_window.groupby([date_col, ticker_col])[size_mid_col].sum().reset_index()
    
    # Pivot to wide format: dates as rows, tickers as columns
    pivot = grouped.pivot(
        index=date_col, 
        columns=ticker_col, 
        values=size_mid_col
    ).fillna(0.0)
    
    # Compute daily totals across all tickers
    totals = pivot.sum(axis=1)
    
    # Compute percentage weights (0-100 scale)
    # Replace zero totals with NaN to avoid division by zero, then fill NaN weights with 0
    weights = pivot.div(totals.replace(0, np.nan), axis=0) * PERCENT_SCALE
    weights = weights.fillna(0.0)
    
    # Combine into output DataFrame
    result = weights.copy()
    result.insert(0, OUTPUT_TOTAL_COL, totals)
    
    # Reset index to make date a column
    result = result.reset_index()
    result = result.rename(columns={date_col: OUTPUT_DATE_COL})
    
    return result


def reindex_full_window(
    out_df: pd.DataFrame, 
    full_index: pd.DatetimeIndex
) -> pd.DataFrame:
    """
    Reindex to ensure all days in window are present, filling missing days with zeros.
    
    Args:
        out_df: Aggregated DataFrame with potentially missing dates
        full_index: Complete date range for window
    
    Returns:
        Reindexed DataFrame with all dates in window
    """
    if len(out_df) == 0:
        # Create empty DataFrame with all dates
        result = pd.DataFrame({OUTPUT_DATE_COL: full_index})
        result[OUTPUT_TOTAL_COL] = 0.0
        return result
    
    # Set date as index for reindexing
    out_df = out_df.set_index(OUTPUT_DATE_COL)
    
    # Reindex to full date range, filling missing with 0
    out_df = out_df.reindex(full_index, fill_value=0.0)
    
    # Reset index back to column
    out_df = out_df.reset_index()
    out_df = out_df.rename(columns={"index": OUTPUT_DATE_COL})
    
    # Sort columns: Date, Total, then alphabetical tickers
    ticker_cols = sorted([col for col in out_df.columns 
                         if col not in [OUTPUT_DATE_COL, OUTPUT_TOTAL_COL]])
    column_order = [OUTPUT_DATE_COL, OUTPUT_TOTAL_COL] + ticker_cols
    
    return out_df[column_order]


def get_aggregated_window(
    date_input: Union[str, pd.Timestamp],
    window_days: int = DEFAULT_DATE_WINDOW_DAYS,
    data_path: Path = DATA_PATH
) -> pd.DataFrame:
    """
    Get aggregated congressional trading data for a date window.
    
    Main function that orchestrates the full data processing pipeline:
    1. Parses input date
    2. Loads congressional trading data
    3. Parses trade size midpoints
    4. Filters to date window
    5. Aggregates by date and ticker
    6. Computes ticker percentage weights
    7. Ensures all dates in window are present
    
    Args:
        date_input: Center date for window (string or Timestamp)
        window_days: Number of days before and after date (default 30)
        data_path: Path to congressional trading CSV (default DATA_PATH)
    
    Returns:
        Wide-format DataFrame with:
            - Date: Calendar date
            - Total_Trade_Size_USD: Sum of all trade midpoints for that day
            - Ticker columns: Each ticker's percentage of daily volume (0-100)
    
    Example:
        >>> df = get_aggregated_window("2025-08-26")
        >>> df.head()
                Date  Total_Trade_Size_USD  AAPL   GOOGL  MSFT  ...
        0 2025-07-27              150000.0  15.5   20.3   10.2  ...
    """
    # Parse input date
    anchor_date = parse_date_input(date_input)
    
    # Load data
    df, date_col = load_congress_trades(data_path)
    
    # Parse trade sizes to midpoints
    df["Trade_Size_USD_Mid"] = parse_trade_size_to_mid(df[TRADE_SIZE_COL])
    
    # Drop rows with unparseable trade sizes
    initial_rows = len(df)
    df = df.dropna(subset=["Trade_Size_USD_Mid"])
    dropped_sizes = initial_rows - len(df)
    
    if dropped_sizes > 0:
        warnings.warn(
            f"Dropped {dropped_sizes} rows with unparseable trade sizes"
        )
    
    # Filter to date window
    df_window, full_index = filter_date_window(df, date_col, anchor_date, window_days)
    
    # Aggregate by date and ticker
    result = aggregate_by_date_ticker(
        df_window, 
        date_col, 
        TICKER_COL, 
        "Trade_Size_USD_Mid"
    )
    
    # Reindex to ensure all dates in window are present
    result = reindex_full_window(result, full_index)
    
    return result


def main(
    date_input: Union[str, pd.Timestamp],
    window_days: int = DEFAULT_DATE_WINDOW_DAYS,
    data_path: Path = DATA_PATH,
    output_path: Optional[Union[str, Path]] = None,
    return_df: bool = True
) -> Optional[pd.DataFrame]:
    """
    Main execution function for congressional trading aggregation.
    
    Args:
        date_input: Center date for window
        window_days: Number of days before and after date (default 30)
        data_path: Path to congressional trading CSV (default DATA_PATH)
        output_path: Optional path to save CSV output
        return_df: Whether to return DataFrame (default True)
    
    Returns:
        DataFrame if return_df=True, otherwise None
    """
    # Get aggregated window
    df = get_aggregated_window(date_input, window_days, data_path)
    
    # Save to CSV if output path provided
    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
    
    # Return DataFrame if requested
    if return_df:
        return df
    
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate congressional trading data around a date window"
    )
    
    parser.add_argument(
        "--date",
        required=True,
        help="Center date for window (e.g., '2025-08-26')"
    )
    
    parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_DATE_WINDOW_DAYS,
        help=f"Number of days before and after date (default: {DEFAULT_DATE_WINDOW_DAYS})"
    )
    
    parser.add_argument(
        "--data-path",
        type=str,
        default=str(DATA_PATH),
        help=f"Path to congressional trading CSV (default: {DATA_PATH})"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional path to save CSV output"
    )
    
    args = parser.parse_args()
    
    # Parse date and compute window
    anchor_date = parse_date_input(args.date)
    start_date = anchor_date - pd.Timedelta(days=args.window)
    end_date = anchor_date + pd.Timedelta(days=args.window)
    
    print(f"Aggregating congressional trades:")
    print(f"  Center date: {anchor_date.date()}")
    print(f"  Window: {start_date.date()} to {end_date.date()} ({args.window*2 + 1} days)")
    
    # Run main function
    df = main(
        date_input=args.date,
        window_days=args.window,
        data_path=Path(args.data_path),
        output_path=args.output,
        return_df=True
    )
    
    print(f"\nResult:")
    print(f"  Rows: {len(df)}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Tickers: {len(df.columns) - 2}")
    
    if args.output:
        print(f"\nOutput saved to: {args.output}")
