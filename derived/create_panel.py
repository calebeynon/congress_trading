"""
creates panel data differentiated by event window. To be fed in the DiD regression
author: Caleb
date: 11-14-2025
"""
import pandas as pd
from derived.cong_agg_date import get_aggregated_window
from derived.market_volume_agg_date import get_market_volumes
import json

INPUT = 'data/derived/news_sentiment_with_events_20dayEX.csv'
OUTPUT = 'data/derived/panel.csv'
WINDOW_DAYS = 30

sent = pd.read_csv(INPUT)

mins = sent.loc[sent['local_min'] == 1, 'date'].tolist()
maxs = sent.loc[sent['local_max']==1, 'date'].tolist()

min_df = pd.DataFrame()
min_maps = {}
for i,m in enumerate(mins):
    df = get_aggregated_window(m,window_days = WINDOW_DAYS)
    mar = get_market_volumes(m,window_days = WINDOW_DAYS)
    df['treat'] = f'event_{i}'
    min_maps[i] = m
    df = pd.merge(df[['Date','Total_Trade_Size_USD','treat']],mar,on='Date')
    min_df = pd.concat([min_df, df], ignore_index=True)
    print(f'{i},{m}')

max_df = pd.DataFrame()
max_maps = {}
for i,m in enumerate(maxs):
    df = get_aggregated_window(m,window_days = WINDOW_DAYS)
    mar = get_market_volumes(m,window_days = WINDOW_DAYS)
    df['treat'] = f'event_{i+len(mins)}'
    max_maps[i+len(mins)] = m
    df = pd.merge(df[['Date','Total_Trade_Size_USD','treat']],mar,on='Date')
    max_df = pd.concat([max_df,df],ignore_index = True)
    print(f'{i+len(mins)},{m}')


max_df['event'] = 'max'
min_df['event'] = 'min'

final_df = pd.concat([max_df,min_df],ignore_index = True)
final_df.to_csv(OUTPUT)

with open('data/derived/max_maps.json', 'w') as f:
    json.dump(max_maps, f)

with open('data/derived/min_maps.json', 'w') as f:
    json.dump(min_maps,f)
