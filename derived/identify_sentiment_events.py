"""
Identify extreme sentiment reversal events for DiD analysis.

This script finds local minima and maxima in news sentiment data and selects
the top 5 most extreme reversals of each type per year. Extremity is measured
by the largest single-day change after the local extremum.
"""

import pandas as pd
import numpy as np
import argparse
from scipy.signal import argrelextrema

# Global constants
INPUT_PATH = "data/derived/news_sentiment_filtered.csv"
OUTPUT_PATH = "data/derived/news_sentiment_with_events.csv"
WINDOW_DAYS = 20
TOP_K = 1
REVERSAL_DAYS = 10  # Default: look at 1-day change after extremum
SMOOTHING_WINDOW = 5  # Default: 5-day moving average for smoothing
SENTIMENT_COL_CANDIDATES = ["News.Sentiment", "News Sentiment", "sentiment", "News_Sentiment"]


def load_data(path):
    """
    Load and prepare sentiment data.
    
    Args:
        path: Path to input CSV file
        
    Returns:
        DataFrame with normalized columns
    """
    df = pd.read_csv(path)
    
    # Identify sentiment column
    sentiment_col = None
    for col_name in SENTIMENT_COL_CANDIDATES:
        if col_name in df.columns:
            sentiment_col = col_name
            break
    
    if sentiment_col is None:
        raise ValueError(
            f"Could not find sentiment column. Looked for: {SENTIMENT_COL_CANDIDATES}. "
            f"Available columns: {df.columns.tolist()}"
        )
    
    # Create working copy of sentiment column
    df['sentiment'] = df[sentiment_col].copy()
    
    # Parse date column
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], format='%m/%d/%y')
    elif 'date_clean' in df.columns:
        df['date'] = pd.to_datetime(df['date_clean'])
    else:
        raise ValueError("No 'date' or 'date_clean' column found")
    
    # Sort by date
    df = df.sort_values('date').reset_index(drop=True)
    
    # Ensure year column exists
    if 'yr' not in df.columns:
        df['yr'] = df['date'].dt.year
    else:
        # Convert 2-digit year to 4-digit if needed
        if df['yr'].max() < 100:
            df['yr'] = df['yr'].apply(lambda x: 2000 + x if x < 50 else 1900 + x)
    
    print(f"Loaded {len(df)} observations from {df['date'].min()} to {df['date'].max()}")
    print(f"Years covered: {sorted(df['yr'].unique())}")
    
    return df


def smooth_sentiment(df, sentiment_col='sentiment', smoothing_window=SMOOTHING_WINDOW):
    """
    Smooth sentiment data using a centered rolling mean to reduce noise.
    
    This helps focus on larger sentiment swings rather than daily volatility.
    
    Args:
        df: DataFrame with sentiment data
        sentiment_col: Name of sentiment column to smooth
        smoothing_window: Window size for rolling mean (days)
        
    Returns:
        DataFrame with sentiment_smoothed column added
    """
    df['sentiment_smoothed'] = df[sentiment_col].rolling(
        window=smoothing_window,
        center=True,
        min_periods=1
    ).mean()
    
    print(f"Applied {smoothing_window}-day centered rolling mean for smoothing")
    
    return df


def find_local_extrema(df, sentiment_col='sentiment', window=WINDOW_DAYS):
    """
    Identify local minima and maxima using scipy.signal.argrelextrema.
    
    A point is a local minimum if it's the smallest value within a window of
    neighboring points. A local maximum is identified similarly for peaks.
    This method is more robust than manual iteration.
    
    Args:
        df: DataFrame with sentiment data
        sentiment_col: Name of sentiment column to analyze
        window: Number of days on each side for comparison (order parameter)
        
    Returns:
        DataFrame with is_local_min and is_local_max boolean columns
    """
    s = df[sentiment_col].values
    n = len(s)
    
    # Initialize arrays
    is_local_min = np.zeros(n, dtype=bool)
    is_local_max = np.zeros(n, dtype=bool)
    
    # Find local minima using scipy (comparator: less than)
    min_indices = argrelextrema(s, np.less, order=window)[0]
    is_local_min[min_indices] = True
    
    # Find local maxima using scipy (comparator: greater than)
    max_indices = argrelextrema(s, np.greater, order=window)[0]
    is_local_max[max_indices] = True
    
    df['is_local_min'] = is_local_min
    df['is_local_max'] = is_local_max
    
    n_mins = is_local_min.sum()
    n_maxs = is_local_max.sum()
    print(f"\nFound {n_mins} local minima and {n_maxs} local maxima (window={window} days)")
    
    return df


def compute_extremity_scores(df, sentiment_col='sentiment', reversal_days=REVERSAL_DAYS):
    """
    Calculate extremity scores based on largest N-day reversal after extremum.
    
    For minima: largest positive N-day change after the minimum
    For maxima: largest negative N-day change (in absolute value) after the maximum
    
    Args:
        df: DataFrame with sentiment data and extrema indicators
        sentiment_col: Name of sentiment column
        reversal_days: Number of days to look ahead for the reversal (default: 1)
        
    Returns:
        DataFrame with extremity_score_min and extremity_score_max columns
    """
    s = df[sentiment_col].values
    n = len(s)
    
    # Initialize extremity score arrays
    extremity_min = np.zeros(n)
    extremity_max = np.zeros(n)
    
    # For each point, calculate the maximum N-day change in the future
    for i in range(n - reversal_days):
        # Look at all N-day changes from this point forward
        future_changes = []
        for j in range(i + reversal_days, min(n, i + 200)):  # Look up to 200 days ahead
            if j - reversal_days >= 0:
                n_day_change = s[j] - s[j - reversal_days]
                future_changes.append(n_day_change)
        
        if future_changes:
            # For minima: find largest positive N-day change
            max_positive_change = max([c for c in future_changes if c > 0], default=0)
            extremity_min[i] = max_positive_change
            
            # For maxima: find largest negative N-day change (absolute value)
            min_negative_change = min([c for c in future_changes if c < 0], default=0)
            extremity_max[i] = abs(min_negative_change)
    
    df['extremity_score_min'] = extremity_min
    df['extremity_score_max'] = extremity_max
    
    return df


def select_top_events_by_year(df, top_k=TOP_K):
    """
    Select the top K most extreme minima and maxima for each year.
    
    Args:
        df: DataFrame with extrema and extremity scores
        top_k: Number of events to select per type per year
        
    Returns:
        DataFrame with local_min and local_max binary indicators
    """
    # Initialize indicator columns
    df['local_min'] = 0
    df['local_max'] = 0
    
    # Process each year separately
    for year in sorted(df['yr'].unique()):
        year_mask = df['yr'] == year
        
        # Select top K minima for this year
        min_candidates = df[year_mask & df['is_local_min']].copy()
        if len(min_candidates) > 0:
            min_candidates = min_candidates.sort_values(
                ['extremity_score_min', 'date'],
                ascending=[False, True]
            )
            top_mins = min_candidates.head(top_k)
            df.loc[top_mins.index, 'local_min'] = 1
        
        # Select top K maxima for this year
        max_candidates = df[year_mask & df['is_local_max']].copy()
        if len(max_candidates) > 0:
            max_candidates = max_candidates.sort_values(
                ['extremity_score_max', 'date'],
                ascending=[False, True]
            )
            top_maxs = max_candidates.head(top_k)
            df.loc[top_maxs.index, 'local_max'] = 1
    
    # Print summary
    print("\nSelected events by year:")
    for year in sorted(df['yr'].unique()):
        n_mins = df[(df['yr'] == year) & (df['local_min'] == 1)].shape[0]
        n_maxs = df[(df['yr'] == year) & (df['local_max'] == 1)].shape[0]
        print(f"  {year}: {n_mins} minima, {n_maxs} maxima")
    
    return df


def validate_results(df, top_k=TOP_K):
    """
    Validate that the results meet expected criteria.
    
    Args:
        df: DataFrame with final event indicators
        top_k: Expected maximum events per type per year
    """
    # Check that no year has more than top_k events
    for year in df['yr'].unique():
        year_data = df[df['yr'] == year]
        n_mins = (year_data['local_min'] == 1).sum()
        n_maxs = (year_data['local_max'] == 1).sum()
        
        assert n_mins <= top_k, f"Year {year} has {n_mins} minima (max: {top_k})"
        assert n_maxs <= top_k, f"Year {year} has {n_maxs} maxima (max: {top_k})"
    
    # Check that selected events are actual extrema
    mins_valid = df[df['local_min'] == 1]['is_local_min'].all()
    maxs_valid = df[df['local_max'] == 1]['is_local_max'].all()
    
    assert mins_valid, "Some selected minima are not actual local minima"
    assert maxs_valid, "Some selected maxima are not actual local maxima"
    
    print("\nValidation passed!")


def build_output_and_save(df, output_path=OUTPUT_PATH):
    """
    Prepare final output DataFrame and save to CSV.
    
    Args:
        df: DataFrame with all computed columns
        output_path: Path to save output CSV
    """
    # Create a unified extremity_score column for easy inspection
    df['extremity_score'] = np.nan
    df.loc[df['local_min'] == 1, 'extremity_score'] = df.loc[
        df['local_min'] == 1, 'extremity_score_min'
    ]
    df.loc[df['local_max'] == 1, 'extremity_score'] = df.loc[
        df['local_max'] == 1, 'extremity_score_max'
    ]
    
    # Drop intermediate working columns
    cols_to_drop = ['sentiment', 'sentiment_smoothed', 'is_local_min', 'is_local_max', 
                    'extremity_score_min', 'extremity_score_max']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    total_events = (df['local_min'] == 1).sum() + (df['local_max'] == 1).sum()
    print(f"\nOutput saved to: {output_path}")
    print(f"Total events identified: {total_events}")
    print(f"  Minima: {(df['local_min'] == 1).sum()}")
    print(f"  Maxima: {(df['local_max'] == 1).sum()}")


def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='Identify extreme sentiment reversal events for DiD analysis.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--reversal-days',
        type=int,
        default=REVERSAL_DAYS,
        help='Number of days to look ahead for reversal (N-day change after extremum)'
    )
    
    parser.add_argument(
        '--window-days',
        type=int,
        default=WINDOW_DAYS,
        help='Window size for identifying local extrema (days on each side)'
    )
    
    parser.add_argument(
        '--top-k',
        type=int,
        default=TOP_K,
        help='Number of top events to select per type per year'
    )
    
    parser.add_argument(
        '--smoothing-window',
        type=int,
        default=SMOOTHING_WINDOW,
        help='Window size for rolling mean smoothing (set to 1 to disable)'
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default=INPUT_PATH,
        help='Path to input CSV file'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=OUTPUT_PATH,
        help='Path to output CSV file'
    )
    
    return parser.parse_args()


def main():
    """
    Main execution function.
    """
    # Parse command-line arguments
    args = parse_arguments()
    
    print("=" * 60)
    print("Identifying Extreme Sentiment Reversal Events")
    print("=" * 60)
    print(f"Configuration:")
    print(f"  Reversal window: {args.reversal_days} days")
    print(f"  Extrema window: {args.window_days} days")
    print(f"  Smoothing window: {args.smoothing_window} days")
    print(f"  Top K per year: {args.top_k}")
    print("=" * 60)
    
    # Load and prepare data
    df = load_data(args.input)
    
    # Smooth sentiment data to focus on larger swings
    df = smooth_sentiment(df, 'sentiment', args.smoothing_window)
    
    # Find local extrema on smoothed data
    df = find_local_extrema(df, 'sentiment_smoothed', args.window_days)
    
    # Calculate extremity scores
    df = compute_extremity_scores(df, 'sentiment', args.reversal_days)
    
    # Select top events per year
    df = select_top_events_by_year(df, args.top_k)
    
    # Validate results
    validate_results(df, args.top_k)
    
    # Save output
    build_output_and_save(df, args.output)
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
