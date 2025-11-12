"""
Market Index Volume Aggregation by Date Window

Purpose:
    Aggregate trading volume for major market indices around a specified date,
    providing a comparison baseline for congressional trading activity.

Methodology:
    - Takes an input date and creates a window of ±N days (default 30)
    - Extracts volume data for major market index ETFs:
        * SPY - S&P 500 ETF
        * DIA - Dow Jones Industrial Average ETF
        * QQQ - NASDAQ-100 ETF
        * IWM - Russell 2000 ETF
        * VOO - Vanguard S&P 500 ETF
        * VTI - Vanguard Total Stock Market ETF
    - Returns wide-format DataFrame with one column per index

Assumptions:
    - Input data contains columns: Date, Ticker, Volume
    - Volume is numeric
    - Data is from all_stock_data_filtered_enhanced.csv

Example Usage:
    # Import usage
    from derived.market_volume_agg_date import get_market_volumes
    df = get_market_volumes("2025-08-26")
    
    # Combined with congressional data
    from derived.cong_agg_date import get_aggregated_window
    from derived.market_volume_agg_date import get_market_volumes
    cong_df = get_aggregated_window("2025-08-26")
    market_df = get_market_volumes("2025-08-26")
    
    # CLI usage
    python derived/market_volume_agg_date.py --date 2025-08-26 --window 30

Author: Generated for congressional trading analysis
Date: 2025-11-12
"""

import warnings
from pathlib import Path
from typing import Union, Optional, Tuple
import argparse

import numpy as np
import pandas as pd


# Global constants
STOCK_DATA_PATH = Path("/Users/caleb/Research/congress_trading/data/derived/all_stock_data_filtered_enhanced.csv")
DEFAULT_DATE_WINDOW_DAYS = 30
DATE_COL = "Date"
TICKER_COL = "Ticker"
VOLUME_COL = "Volume"
OUTPUT_DATE_COL = "Date"

# Major market index ETFs to track
INDEX_TICKERS = {
    "SPY": "S&P_500_Volume",
    "DIA": "Dow_Jones_Volume",
    "QQQ": "NASDAQ_100_Volume",
    "IWM": "Russell_2000_Volume",
    "VOO": "Vanguard_SP500_Volume",
    "VTI": "Total_Market_Volume"
}


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


def load_index_volume_data(data_path: Path = STOCK_DATA_PATH) -> pd.DataFrame:
    """
    Load stock data and filter to major market indices.
    
    Args:
        data_path: Path to stock data CSV file
    
    Returns:
        DataFrame with Date, Ticker, and Volume columns for index ETFs
    
    Raises:
        FileNotFoundError: If data file doesn't exist
        KeyError: If required columns are missing
    """
    if not data_path.exists():
        raise FileNotFoundError(f"Stock data file not found: {data_path}")
    
    # Load only necessary columns for efficiency
    required_cols = [DATE_COL, TICKER_COL, VOLUME_COL]
    
    try:
        df = pd.read_csv(
            data_path, 
            usecols=required_cols,
            dtype={TICKER_COL: "string"},
            low_memory=False
        )
    except ValueError as e:
        raise KeyError(f"Required columns missing from stock data: {required_cols}") from e
    
    # Parse dates
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    initial_rows = len(df)
    df = df.dropna(subset=[DATE_COL])
    dropped_dates = initial_rows - len(df)
    
    if dropped_dates > 0:
        warnings.warn(f"Dropped {dropped_dates} rows with invalid dates")
    
    # Filter to index tickers only
    index_ticker_list = list(INDEX_TICKERS.keys())
    df = df[df[TICKER_COL].isin(index_ticker_list)].copy()
    
    if len(df) == 0:
        warnings.warn(
            f"No data found for index tickers: {index_ticker_list}. "
            "Market volume data will be empty."
        )
        return df
    
    # Ensure volume is numeric
    df[VOLUME_COL] = pd.to_numeric(df[VOLUME_COL], errors="coerce")
    df = df.dropna(subset=[VOLUME_COL])
    
    return df


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
    
    # Normalize dates
    df[date_col] = df[date_col].dt.normalize()
    
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


def aggregate_by_date_index(
    df_window: pd.DataFrame, 
    date_col: str, 
    ticker_col: str, 
    volume_col: str
) -> pd.DataFrame:
    """
    Aggregate volumes by date and index ticker.
    
    Args:
        df_window: Filtered DataFrame with index volumes in window
        date_col: Name of date column
        ticker_col: Name of ticker column
        volume_col: Name of volume column
    
    Returns:
        Wide DataFrame with Date and one column per index
    """
    if len(df_window) == 0:
        # Return empty DataFrame with correct structure
        return pd.DataFrame(columns=[OUTPUT_DATE_COL])
    
    # Group by date and ticker, sum volumes (in case of duplicates)
    grouped = df_window.groupby([date_col, ticker_col])[volume_col].sum().reset_index()
    
    # Pivot to wide format: dates as rows, tickers as columns
    pivot = grouped.pivot(
        index=date_col, 
        columns=ticker_col, 
        values=volume_col
    )
    
    # Rename columns using friendly names from INDEX_TICKERS
    pivot = pivot.rename(columns=INDEX_TICKERS)
    
    # Reset index to make date a column
    pivot = pivot.reset_index()
    pivot = pivot.rename(columns={date_col: OUTPUT_DATE_COL})
    
    return pivot


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
        # Add columns for each index with zeros
        for friendly_name in INDEX_TICKERS.values():
            result[friendly_name] = 0.0
        return result
    
    # Set date as index for reindexing
    out_df = out_df.set_index(OUTPUT_DATE_COL)
    
    # Reindex to full date range, filling missing with 0
    out_df = out_df.reindex(full_index, fill_value=0.0)
    
    # Reset index back to column
    out_df = out_df.reset_index()
    out_df = out_df.rename(columns={"index": OUTPUT_DATE_COL})
    
    # Sort columns: Date first, then alphabetical indices
    index_cols = sorted([col for col in out_df.columns if col != OUTPUT_DATE_COL])
    column_order = [OUTPUT_DATE_COL] + index_cols
    
    # Only use columns that exist
    column_order = [col for col in column_order if col in out_df.columns]
    
    return out_df[column_order]


def get_market_volumes(
    date_input: Union[str, pd.Timestamp],
    window_days: int = DEFAULT_DATE_WINDOW_DAYS,
    data_path: Path = STOCK_DATA_PATH
) -> pd.DataFrame:
    """
    Get market index volume data for a date window.
    
    Main function that orchestrates the full data processing pipeline:
    1. Parses input date
    2. Loads stock data for major indices
    3. Filters to date window
    4. Aggregates by date and index
    5. Ensures all dates in window are present
    
    Args:
        date_input: Center date for window (string or Timestamp)
        window_days: Number of days before and after date (default 30)
        data_path: Path to stock data CSV (default STOCK_DATA_PATH)
    
    Returns:
        Wide-format DataFrame with:
            - Date: Calendar date
            - Index columns: Volume for each major market index
    
    Example:
        >>> df = get_market_volumes("2025-08-26")
        >>> df.head()
                Date  Dow_Jones_Volume  NASDAQ_100_Volume  S&P_500_Volume  ...
        0 2025-07-27          5234000           45123000        78456000  ...
    """
    # Parse input date
    anchor_date = parse_date_input(date_input)
    
    # Load index volume data
    df = load_index_volume_data(data_path)
    
    # Filter to date window
    df_window, full_index = filter_date_window(df, DATE_COL, anchor_date, window_days)
    
    # Aggregate by date and index
    result = aggregate_by_date_index(
        df_window, 
        DATE_COL, 
        TICKER_COL, 
        VOLUME_COL
    )
    
    # Reindex to ensure all dates in window are present
    result = reindex_full_window(result, full_index)
    
    return result


def main(
    date_input: Union[str, pd.Timestamp],
    window_days: int = DEFAULT_DATE_WINDOW_DAYS,
    data_path: Path = STOCK_DATA_PATH,
    output_path: Optional[Union[str, Path]] = None,
    return_df: bool = True
) -> Optional[pd.DataFrame]:
    """
    Main execution function for market volume aggregation.
    
    Args:
        date_input: Center date for window
        window_days: Number of days before and after date (default 30)
        data_path: Path to stock data CSV (default STOCK_DATA_PATH)
        output_path: Optional path to save CSV output
        return_df: Whether to return DataFrame (default True)
    
    Returns:
        DataFrame if return_df=True, otherwise None
    """
    # Get market volumes
    df = get_market_volumes(date_input, window_days, data_path)
    
    # Save to CSV if output path provided
    if output_path is not None:
        output_path = Path(output_path)
        df.to_csv(output_path, index=False)
    
    # Return DataFrame if requested
    if return_df:
        return df
    
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate market index volume data around a date window"
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
        default=str(STOCK_DATA_PATH),
        help=f"Path to stock data CSV (default: {STOCK_DATA_PATH})"
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
    
    print(f"Aggregating market index volumes:")
    print(f"  Center date: {anchor_date.date()}")
    print(f"  Window: {start_date.date()} to {end_date.date()} ({args.window*2 + 1} days)")
    print(f"  Indices: {', '.join(INDEX_TICKERS.keys())}")
    
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
    print(f"  Indices tracked: {len(df.columns) - 1}")
    
    if len(df) > 0:
        print(f"\n  Available indices:")
        for col in df.columns:
            if col != OUTPUT_DATE_COL:
                non_zero = (df[col] > 0).sum()
                total_volume = df[col].sum()
                print(f"    - {col}: {non_zero}/{len(df)} days with data, {total_volume:,.0f} total volume")
    
    if args.output:
        print(f"\nOutput saved to: {args.output}")
