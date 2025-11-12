"""
Visualize news sentiment time series with identified extreme events.

This script creates plots showing the sentiment data with local minima and maxima
highlighted to verify the event identification logic.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import argparse
import os

# Global constants
INPUT_PATH = "data/derived/news_sentiment_with_events.csv"
OUTPUT_DIR = "output/figures"


def load_event_data(path):
    """
    Load sentiment data with event markers.
    
    Args:
        path: Path to CSV with sentiment and event indicators
        
    Returns:
        DataFrame with parsed dates
    """
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    
    print(f"Loaded {len(df)} observations")
    print(f"Events: {(df['local_min'] == 1).sum()} minima, {(df['local_max'] == 1).sum()} maxima")
    
    return df


def plot_full_time_series(df):
    """
    Create overview plot of entire time series with events.
    
    Args:
        df: DataFrame with sentiment and event indicators
    """
    fig, ax = plt.subplots(figsize=(16, 6))
    
    # Plot sentiment line
    ax.plot(df['date'], df['News.Sentiment'], 
            color='steelblue', linewidth=0.8, alpha=0.7, label='News Sentiment')
    
    # Highlight minima
    minima = df[df['local_min'] == 1]
    ax.scatter(minima['date'], minima['News.Sentiment'], 
               color='red', s=50, marker='v', alpha=0.8, 
               label=f'Local Minima (n={len(minima)})', zorder=5)
    
    # Highlight maxima
    maxima = df[df['local_max'] == 1]
    ax.scatter(maxima['date'], maxima['News.Sentiment'], 
               color='green', s=50, marker='^', alpha=0.8, 
               label=f'Local Maxima (n={len(maxima)})', zorder=5)
    
    # Formatting
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('News Sentiment', fontsize=12, fontweight='bold')
    ax.set_title('News Sentiment Time Series with Extreme Events', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='best', framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    return fig


def plot_yearly_panels(df, years_to_plot=None):
    """
    Create multi-panel plot showing detailed view by year.
    
    Args:
        df: DataFrame with sentiment and event indicators
        years_to_plot: List of years to plot (default: first 6 years)
    """
    if years_to_plot is None:
        years_to_plot = sorted(df['yr'].unique())[:6]
    
    n_years = len(years_to_plot)
    n_cols = 2
    n_rows = (n_years + 1) // 2
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 4 * n_rows))
    axes = axes.flatten()
    
    for idx, year in enumerate(years_to_plot):
        ax = axes[idx]
        year_data = df[df['yr'] == year].copy()
        
        # Plot sentiment line
        ax.plot(year_data['date'], year_data['News.Sentiment'], 
                color='steelblue', linewidth=1.2, alpha=0.7)
        
        # Highlight minima
        minima = year_data[year_data['local_min'] == 1]
        if len(minima) > 0:
            ax.scatter(minima['date'], minima['News.Sentiment'], 
                      color='red', s=80, marker='v', alpha=0.9, 
                      label=f'Minima ({len(minima)})', zorder=5)
        
        # Highlight maxima
        maxima = year_data[year_data['local_max'] == 1]
        if len(maxima) > 0:
            ax.scatter(maxima['date'], maxima['News.Sentiment'], 
                      color='green', s=80, marker='^', alpha=0.9, 
                      label=f'Maxima ({len(maxima)})', zorder=5)
        
        # Formatting
        ax.set_xlabel('Date', fontsize=10)
        ax.set_ylabel('Sentiment', fontsize=10)
        ax.set_title(f'Year {year}', fontsize=11, fontweight='bold')
        ax.legend(loc='best', fontsize=9, framealpha=0.9)
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Hide unused subplots
    for idx in range(n_years, len(axes)):
        axes[idx].set_visible(False)
    
    plt.suptitle('News Sentiment by Year with Extreme Events', 
                 fontsize=14, fontweight='bold', y=1.00)
    plt.tight_layout()
    return fig


def plot_event_details(df, n_examples=6):
    """
    Create detailed zoom-in plots of individual events.
    
    Args:
        df: DataFrame with sentiment and event indicators
        n_examples: Number of example events to show (3 min + 3 max)
    """
    # Get top extremity events
    minima = df[df['local_min'] == 1].nlargest(n_examples // 2, 'extremity_score')
    maxima = df[df['local_max'] == 1].nlargest(n_examples // 2, 'extremity_score')
    
    events = pd.concat([minima, maxima]).sort_values('date')
    
    n_cols = 2
    n_rows = (len(events) + 1) // 2
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 4 * n_rows))
    axes = axes.flatten()
    
    for idx, (event_idx, event_row) in enumerate(events.iterrows()):
        ax = axes[idx]
        event_date = event_row['date']
        event_type = 'Minimum' if event_row['local_min'] == 1 else 'Maximum'
        extremity = event_row['extremity_score']
        
        # Get window around event (±30 days)
        window_start = event_date - pd.Timedelta(days=30)
        window_end = event_date + pd.Timedelta(days=30)
        window_data = df[(df['date'] >= window_start) & (df['date'] <= window_end)]
        
        # Plot sentiment line
        ax.plot(window_data['date'], window_data['News.Sentiment'], 
                color='steelblue', linewidth=1.5, alpha=0.7)
        
        # Highlight the event
        color = 'red' if event_type == 'Minimum' else 'green'
        marker = 'v' if event_type == 'Minimum' else '^'
        ax.scatter([event_date], [event_row['News.Sentiment']], 
                  color=color, s=150, marker=marker, alpha=0.9, 
                  edgecolors='black', linewidths=2, zorder=5)
        
        # Add vertical line at event
        ax.axvline(event_date, color=color, linestyle='--', alpha=0.5, linewidth=2)
        
        # Formatting
        ax.set_xlabel('Date', fontsize=10)
        ax.set_ylabel('Sentiment', fontsize=10)
        title = f'{event_type}: {event_date.strftime("%Y-%m-%d")}\n'
        title += f'Sentiment: {event_row["News.Sentiment"]:.3f}, Extremity: {extremity:.3f}'
        ax.set_title(title, fontsize=10, fontweight='bold')
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Hide unused subplots
    for idx in range(len(events), len(axes)):
        axes[idx].set_visible(False)
    
    plt.suptitle('Detailed View of Most Extreme Events (±30 days)', 
                 fontsize=14, fontweight='bold', y=1.00)
    plt.tight_layout()
    return fig


def save_figure(fig, filename, output_dir=OUTPUT_DIR):
    """
    Save figure to output directory.
    
    Args:
        fig: Matplotlib figure object
        filename: Output filename
        output_dir: Directory to save figure
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_path}")


def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='Visualize sentiment events for verification.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default=INPUT_PATH,
        help='Path to input CSV file with event markers'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default=OUTPUT_DIR,
        help='Directory to save output figures'
    )
    
    parser.add_argument(
        '--suffix',
        type=str,
        default='',
        help='Suffix to add to output filenames (e.g., "_5day")'
    )
    
    return parser.parse_args()


def main():
    """
    Main execution function.
    """
    # Parse arguments
    args = parse_arguments()
    
    print("=" * 60)
    print("Visualizing Sentiment Events")
    print("=" * 60)
    print(f"Input: {args.input}")
    print(f"Output directory: {args.output_dir}")
    print("=" * 60)
    
    # Load data
    df = load_event_data(args.input)
    
    # Create full time series plot
    print("\nCreating full time series plot...")
    fig1 = plot_full_time_series(df)
    save_figure(fig1, "sentiment_events_full_series.png")
    
    # Create yearly panel plots
    print("\nCreating yearly panel plots...")
    fig2 = plot_yearly_panels(df, years_to_plot=[2012, 2013, 2014, 2015, 2016, 2017])
    save_figure(fig2, "sentiment_events_yearly_2012_2017.png")
    
    fig3 = plot_yearly_panels(df, years_to_plot=[2018, 2019, 2020, 2021, 2022, 2023])
    save_figure(fig3, "sentiment_events_yearly_2018_2023.png")
    
    # Create detailed event plots
    print("\nCreating detailed event plots...")
    fig4 = plot_event_details(df, n_examples=6)
    save_figure(fig4, "sentiment_events_detail.png")
    
    print("\n" + "=" * 60)
    print("Done! Check output/figures/ for visualizations")
    print("=" * 60)
    
    # Show plots
    plt.show()


if __name__ == "__main__":
    main()
