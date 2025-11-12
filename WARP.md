# Congress Trading and News Sentiment - Difference-in-Differences Analysis

## Project Overview
This is a causal inference project examining whether stock trading volume for congress members increases **PRIOR** to a spike in news sentiment. We employ a difference-in-differences (DiD) methodology to identify anticipatory trading patterns that may indicate informational advantages.

## Research Question
**Does stock trading volume for congressional holdings increase in advance of news sentiment spikes?**

This addresses whether members of Congress trade on information before it becomes publicly reflected in news sentiment, potentially violating insider trading principles or ethical norms.

## Data Sources

### 1. Congressional Trading Data
- **Path**: `/Users/caleb/Research/congress_trading/data/raw/congress_trading.csv`
- **Observations**: ~104,645 trades
- **Time Coverage**: 2012-02-27 to 2025-09-03
- **Unique Congress Members**: ~600
- **Key Variables**:
  - `Ticker`: Stock ticker symbol
  - `Traded`: Date of trade execution
  - `Transaction`: Type (Purchase/Sale)
  - `Trade_Size_USD`: Range of trade value
  - `Name`: Congress member name
  - `BioGuideID`: Unique identifier for each member
  - `Party`: Political party (R/D)
  - `Chamber`: House or Senate
  - `Filed`: Date trade was filed/disclosed
  - `excess_return`: Excess returns calculation (potentially useful for outcomes)
  - `State`: State represented

### 2. News Sentiment Data
- **Path**: `/Users/caleb/Research/congress_trading/data/raw/new_sentiment_data.csv`
- **Observations**: ~16,696 daily sentiment scores
- **Time Coverage**: 1980-01-01 to 2025-10-05
- **Key Variables**:
  - `date`: Daily timestamp
  - `News Sentiment`: Continuous sentiment score (appears to be negative values, range ~-0.04 to -0.72 visible in sample)
- **Note**: This appears to be **aggregate market-level sentiment**, not ticker-specific

### 3. Stock Volume Data
- **Path**: `/Users/caleb/Research/congress_trading/data/raw/all_stock_data.csv`
- **Observations**: ~34.6 million daily stock records
- **Time Coverage**: 1962-01-02 to 2024-11-04
- **Unique Tickers**: ~9,316
- **Key Variables**:
  - `Date`: Trading date
  - `Ticker`: Stock ticker symbol
  - `Open`, `High`, `Low`, `Close`: Daily OHLC prices
  - `Volume`: **Daily trading volume** (PRIMARY TREATMENT VARIABLE)
  - `Dividends`, `Stock Splits`: Corporate actions

## Methodology Framework

### Difference-in-Differences Design

#### Treatment Definition
We define "treatment" as periods leading up to significant positive spikes in news sentiment. The analysis will examine whether:
1. **Trading volume** of stocks held by Congress members increases in the pre-spike period
2. This increase is **differential** compared to appropriate counterfactual stocks

#### Counterfactual Options
Several potential control groups to explore:

1. **Index Fund Volume (e.g., SPY, VTI)** 
   - Standard market benchmark
   - Reflects general market trading activity
   
2. **Industry-Matched Stocks**
   - Match each congressional trade to similar stocks in the same sector not held by Congress
   - Controls for industry-specific volume trends
   
3. **Synthetic Control**
   - Create weighted portfolio of non-congressional stocks matching pre-treatment characteristics
   
4. **Same Stock, Different Time Period**
   - Use the same stock's volume in non-treatment periods as control

### Unit of Analysis Options

Given the research question, we have several analytical approaches:

#### Option 1: Individual Congress Member Level (RECOMMENDED)
**Analysis**: Compare each member's portfolio stocks to counterfactual during sentiment spike periods

**Advantages**:
- Captures individual-level variation in information access
- Allows identification of specific members with suspicious patterns
- Can control for member-specific fixed effects
- Enables heterogeneity analysis (by party, committee membership, seniority)

**Challenges**:
- Requires member-level portfolio construction
- Variable exposure to different stocks across members
- Need to account for filing lag (gap between `Traded` and `Filed` dates)

**Statistical Approach**:
```
Volume_{ist} = β₀ + β₁(PreSpike_{t}) + β₂(CongressHeld_{is}) + β₃(PreSpike_t × CongressHeld_{is}) + α_i + γ_t + ε_{ist}
```
Where:
- `i` indexes congress members
- `s` indexes stocks
- `t` indexes time
- `PreSpike_t` = indicator for pre-sentiment-spike window
- `CongressHeld_{is}` = indicator for stock held by member i
- `α_i` = member fixed effects
- `γ_t` = time fixed effects

**Standard Errors**: Cluster at member level to account for serial correlation in individual portfolios

#### Option 2: Congress as Whole vs. Control
**Analysis**: Aggregate all congressional holdings and compare to market index

**Advantages**:
- Simpler implementation
- Higher statistical power
- Clear policy interpretation

**Challenges**:
- Loses individual variation
- May dilute signal if only subset of members trade strategically
- Less granular insights

**Standard Errors**: Cluster at time level or use Newey-West with appropriate lag structure

#### Option 3: Stock-Level Analysis
**Analysis**: For each stock, compare volume when held by Congress vs. not held

**Advantages**:
- Controls for stock-specific characteristics
- Large sample size

**Challenges**:
- Treatment (Congress holding) is not randomly assigned
- Selection bias concerns

### Recommended Approach: Hybrid Multi-Level Analysis

**Primary Specification**: Individual member-level DiD with member and time fixed effects
- Allows identification of heterogeneous effects
- Most aligned with research question about anticipatory trading

**Robustness Checks**:
1. Aggregate Congress vs. Index
2. Event study design around sentiment spikes
3. Vary definition of "pre-spike" window (t-1, t-3, t-5, t-7 days)
4. Placebo tests using random time periods

### Key Methodological Considerations

#### 1. Sentiment Spike Definition
**Challenge**: News sentiment data appears to be market-wide, not ticker-specific

**Solutions**:
- Define "spike" as periods when sentiment increases by >X standard deviations
- Use change in sentiment (Δsentiment) rather than levels
- Consider using percentile-based definitions (e.g., top 10% of daily sentiment changes)
- Examine both positive spikes (sentiment improvement) and negative spikes

#### 2. Pre-Spike Window
**Recommendation**: Test multiple windows (3, 5, 7, 10 trading days before spike)
- Too short: May miss anticipatory trading
- Too long: Dilutes treatment effect

#### 3. Standard Error Estimation
**For member-level analysis**:
- Cluster at member level (accounts for serial correlation within portfolios)
- Two-way clustering (member × time) if sufficient variation
- Wild bootstrap for small number of clusters

**For aggregate analysis**:
- Newey-West HAC standard errors
- Block bootstrap

#### 4. Parallel Trends Assumption
**Critical**: Test whether pre-treatment volume trends are parallel

**Diagnostics**:
- Event study plots showing leads and lags
- Formal tests of pre-treatment trend differences
- If violated, consider alternative identification (e.g., synthetic DiD, changes-in-changes)

#### 5. Data Alignment Issues
**Challenge**: Congressional trades have filing lag
- Use `Traded` date (actual transaction) not `Filed` date
- Account for disclosure lag in interpretation (public couldn't know in real-time)

#### 6. Volume Normalization
**Recommendation**: Use log(volume) or normalized volume metrics
- Accounts for heteroskedasticity
- Different stocks have vastly different volume levels
- Consider volume relative to 30-day moving average

### Expected Outcomes and Interpretation

**Positive DiD Coefficient** (β₃ > 0): 
- Congressional stocks show abnormal volume increases before sentiment spikes
- Suggestive of anticipatory trading/information advantage

**Null Effect** (β₃ ≈ 0):
- No differential anticipatory trading pattern
- Either no information advantage or not reflected in volume

**Negative DiD Coefficient** (β₃ < 0):
- Unexpected; could indicate strategic timing to avoid suspicion

### Power Considerations
With:
- ~600 Congress members
- ~104,000 trades
- 60+ years of stock data
- Daily observations

**Power is likely adequate** IF:
- Effect size is meaningful (>10-15% differential volume increase)
- Sufficient sentiment spike events
- Treatment group has meaningful size during each event

### Extensions and Heterogeneity

Explore differential effects by:
1. **Party**: Do Republicans vs. Democrats show different patterns?
2. **Committee Membership**: Finance, Intelligence, Armed Services committees
3. **Seniority**: Longer-serving members may have more information
4. **Trade Size**: Larger trades may reflect higher confidence
5. **Transaction Type**: Purchases vs. Sales
6. **Chamber**: House vs. Senate
7. **Time Period**: Pre- vs. post-STOCK Act (2012)

## Data Considerations

### Challenges
1. **Sentiment granularity**: Market-wide rather than stock-specific
2. **Endogeneity**: Congress members may cause sentiment changes through their actions
3. **Selection bias**: We observe trades, not holdings
4. **Reporting lag**: Time between trade execution and public disclosure
5. **Measurement**: Volume may not fully capture sophisticated trading strategies

### Required Data Processing
1. Merge congressional trades with stock volume data by ticker and date
2. Identify sentiment spike events from time series
3. Create pre-spike indicators (multiple window lengths)
4. Construct control group (index funds or matched stocks)
5. Handle missing data (not all tickers have full history)
6. Normalize volumes for cross-stock comparability

## Software and Implementation
- **Language**: Python (as per user rules)
- **Key Libraries**: pandas, statsmodels, linearmodels (for fixed effects), matplotlib/seaborn
- **Estimation**: Use fixed effects regression with robust standard errors
- **Visualization**: Event study plots, parallel trends diagnostics, coefficient plots

### Script Organization
- **All scripts should go in the `derived` or `analysis` folder**
- If uncertain which folder to use, ask before creating the script
- The `derived` folder is for data processing and feature engineering scripts
- The `analysis` folder is for statistical analysis and modeling scripts

## References and Background
- STOCK Act (2012): Required disclosure of congressional trades
- Prior literature on congressional trading and abnormal returns
- DiD methodology resources (Angrist & Pischke, Goodman-Bacon decomposition)
