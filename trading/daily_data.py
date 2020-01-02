import os
from dotenv import load_dotenv
import requests
import numpy as np
import pandas as pd
import string
import time
from datetime import datetime
from alpha_vantage.timeseries import TimeSeries
from google.cloud import bigquery
from bs4 import BeautifulSoup
from historical_data import get_symbols, load_to_gbq, attempts

load_dotenv()

def perform(market, instrument_type):
  today = datetime.today().strftime('%Y-%m-%d')
  print('Current day: {}'.format(today))

  # Retrieve daily quotes for symbols
  ts = TimeSeries(
    key=os.environ.get('AV_API_KEY'),
    output_format='pandas',
    indexing_type='integer'
  )
  symbols = get_symbols(market)

  df_to_load = pd.DataFrame(columns=[''])

  for symbol in symbols:
    for attempt in range(attempts):
      try:
        df_symbol, metadata = ts.get_quote_endpoint(
          symbol=symbol
        )
      except Exception as e:
        print('Something went wrong for {} {}. Error: {}'.format(instrument_type, symbol, e))
        time.sleep(10)
      else:
        print(df_symbol)
        if not df_to_load.empty:
          df_to_load = pd.concat([df_to_load, df_symbol], ignore_index=True, sort =False)
        else:
          df_to_load = df_symbol
        break
    else:
      print('All {} attempts to retrieve data failed.'.format(attempts))

  # Dataframe formating
  for column in df_to_load.columns:
    df_to_load = df_to_load.rename({column: column[4:]}, axis='columns')

  df_to_load.drop(['previous close', 'change', 'change percent'], axis=1, inplace=True)
  df_to_load = df_to_load.rename({'latest trading day': 'date', 'price': 'close'}, axis='columns')
  df_to_load['date'] = pd.to_datetime(df_to_load['date'])
  df_to_load['date'] = df_to_load['date'].dt.strftime('%Y-%m-%d')
  df_to_load = df_to_load.reset_index(drop=True)
  df_to_load = df_to_load[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
  df_to_load = df_to_load.loc[df_to_load['date'] == today]

  load_to_gbq(
    df_to_load,
    '{}_dataset'.format(instrument_type.lower()),
    '{}_quotes'.format(market.lower())
  )

if __name__ == '__main__':
  start = time.time()
  perform('nyse', 'equity')
  end = time.time()
  difference = int(end - start)
  print('{} hours to retrieve daily data.'.format(difference / 3600))
