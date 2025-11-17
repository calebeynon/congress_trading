# Identify Sentiment Events Script

## Purpose

`identify_sentiment_events.py` identifies extreme sentiment reversal events in news sentiment data for use in difference-in-differences (DiD) analysis. The script finds local minima and maxima in sentiment time series and selects the most extreme events based on the magnitude of subsequent reversals.

## Input/Output

- **Input**: `data/derived/news_sentiment_filtered.csv` (news sentiment time series)
- **Output**: `data/derived/news_sentiment_with_events.csv` (sentiment data with `local_min` and `local_max` binary indicators)

## Algorithm Overview

### 1. Data Smoothing

- Applies a centered rolling mean (default: 5 days) to reduce noise
- Focuses on larger sentiment swings rather than daily volatility

### 2. Local Extrema Identification

Uses `scipy.signal.argrelextrema` to find local minima and maxima:
- A point is a **local minimum** if it's the smallest value within a window (default: 20 days on each side)
- A point is a **local maximum** if it's the largest value within the same window

### 3. Extremity Scoring

Events are scored by the magnitude of the sentiment reversal that follows them:

- **For minima**: Extremity = largest positive N-day change after the minimum (where N defaults to 10 days)
- **For maxima**: Extremity = largest negative N-day change (absolute value) after the maximum

Higher scores indicate more dramatic reversals, suggesting more significant sentiment shifts.

### 4. Top-K Selection Per Year

- For each year, selects the top K most extreme minima (default: K=1)
- For each year, selects the top K most extreme maxima (default: K=1)
- Events are ranked by extremity score, with ties broken by date (earlier wins)

### 5. Minimum Separation Enforcement

After top-K selection, enforces a **30-day minimum separation** between all sentiment events:

**Algorithm**: Greedy chronological sweep with replacement
1. Sort all selected events (minima and maxima combined) by date
2. Process events sequentially:
   - Keep the first event
   - For each subsequent event:
     - If ≥30 days from the last kept event: **keep it**
     - If <30 days from the last kept event: **conflict**
       - Compare extremity scores
       - Keep the event with the higher score
       - Remove the event with the lower score
       - If scores are equal, keep the earlier event

**Key Properties**:
- Applies across all dates, including year boundaries
- Events of different types (min vs. max) are still constrained by the 30-day rule
- A previously kept event can be replaced if a more extreme event appears within 30 days
- May result in fewer than K events per year after enforcement

## Command-Line Options

```bash
python derived/identify_sentiment_events.py [OPTIONS]
```

**Options**:
- `--reversal-days N`: Days to look ahead for reversal magnitude (default: 10)
- `--window-days N`: Window size for local extrema identification (default: 20)
- `--top-k K`: Number of events per type per year (default: 1)
- `--smoothing-window N`: Rolling mean window (default: 5, set to 1 to disable)
- `--input PATH`: Custom input file path
- `--output PATH`: Custom output file path

## Example Usage

```bash
# Use default settings (1 event per type per year, 30-day separation)
python derived/identify_sentiment_events.py

# Select top 3 events per year with 60-day separation (requires manual constant change)
python derived/identify_sentiment_events.py --top-k 3

# Use longer reversal window to capture sustained shifts
python derived/identify_sentiment_events.py --reversal-days 20
```

## Output Columns

- `local_min`: Binary indicator (1 = selected minimum event, 0 = not selected)
- `local_max`: Binary indicator (1 = selected maximum event, 0 = not selected)
- `extremity_score`: Score for selected events (NaN for non-events)
- All original columns from input data preserved

## Notes

- The minimum separation constraint (`MIN_SEPARATION_DAYS = 30`) is a global constant in the script
- Validation checks ensure selected events are actual local extrema and respect the top-K constraint
- The script prints diagnostic information including counts of events identified, selected, and removed
