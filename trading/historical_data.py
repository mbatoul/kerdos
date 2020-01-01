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

load_dotenv()

attempts = 3
rows = 0
count = 0

def increment_count(int):
  global count
  count += int

def increment_rows(int):
  global rows
  rows += int

def chunks(l, n):
  n = max(1, n)

  return (l[i:i+n] for i in range(0, len(l), n))

# Retrieves all ticker symbols of give market.
def get_symbols(market):
  alphabet = list(string.ascii_uppercase)

  symbols = []

  print('Start parsing eoddata to retrieve symbols...')
  for each in alphabet:
    url = 'http://eoddata.com/stocklist/{}/{}.htm'.format(market, each)
    resp = requests.get(url)
    site = resp.content
    soup = BeautifulSoup(site, 'html.parser')
    table = soup.find('table', {'class': 'quotes'})
    for row in table.findAll('tr')[1:]:
      symbol = row.findAll('td')[0].text.rstrip()
      symbols.append(symbol)
      print('Get {}...'.format(symbol))

  symbols_clean = []

  print('Start cleaning the symbols...')
  for each in symbols:
    each = each.replace('.', '-')
    each = each.split('-')[0]
    symbols_clean.append(each)

  symbols_clean = list(set(symbols_clean))

  print('{} {} ticker symbols retrieved and cleaned.'.format(market.upper(), len(symbols_clean)))

  return symbols_clean

# Retrieves +20 years for historical quotes for a single stock.
def retrieve_from_av(symbol):
  print('Start retrieving historical time series...')

  df = pd.DataFrame(columns=[''])

  for attempt in range(attempts):
    try:
      ts = TimeSeries(
        key=os.environ.get('AV_API_KEY'),
        output_format='pandas',
        indexing_type='integer'
      )

      df, metadata = ts.get_daily(
        symbol=symbol,
        outputsize='full'
      )
    except Exception as e:
      print('Attempt {} to retrieve data failed. Error: {}'.format(attempt + 1, e))
      time.sleep(10)
    else:
      # Clean column names (remove numbers)
      for column in df.columns[1:]:
        df = df.rename({column: column[3:]}, axis='columns')

      # Format date to datetime
      df = df.rename({'index': 'date'}, axis='columns')
      df['date'] = pd.to_datetime(df['date'])
      df['date'] = df['date'].dt.strftime('%Y-%m-%d')

      # # Add symbol column
      df.insert(loc=0, column='symbol', value=symbol)

      print('Retrieval of {} time series succeeded!'.format(len(df.index)))
      break
  else:
    print('All {} attempts to retrieve data failed.'.format(attempts))

  return df

# Uploads data to BigQuery table.
def load_to_gbq(dataframe, dataset_id, table_id):
  if dataframe.empty:
    print('No data to load.')
    return

  res = None

  for attempt in range(attempts):
    try:
      client = bigquery.Client()

      dataset_ref = client.dataset(dataset_id)
      table_ref = dataset_ref.table(table_id)

      job_config = bigquery.LoadJobConfig()
      job_config.source_format = bigquery.SourceFormat.PARQUET
      job_config.write_disposition = 'WRITE_APPEND'
      job_config.autodetect = True

      print('Start upload to BigQuery...')
      job = client.load_table_from_dataframe(
        dataframe,
        table_ref,
        location='EU',
        job_config=job_config
      )

    except Exception as e:
      print('Attempt {} to load data failed. Error: {}'.format(attempt + 1, e))
      time.sleep(10)
    else:
      increment_rows(len(dataframe.index))
      print('Upload succeeded!')
      res = 'Upload succeeded'
      break
  else:
    res = 'Upload failed'
    print('All {} attemps to load data failed'.format(attempts))

  return res

def perform(market, instrument_type):
  start = time.time()
  symbols = get_symbols(market)
  nb_of_loads = int(round(len(symbols) / 1000) + 1)
  symbols_chunked = list(chunks(list(set(symbols)), nb_of_loads))

  for chunk_of_symbols in symbols_chunked:
    df_to_load = pd.DataFrame(columns=[''])

    for symbol in chunk_of_symbols:
      print('------------------- Stock: {} -------------------'.format(symbol))

      df = retrieve_from_av(symbol)
      if not df.empty:
        if df_to_load.empty:
          df_to_load = df
        else:
          df_to_load = pd.concat([df_to_load, df], ignore_index=True, sort =False)
        increment_count(1)

    load_to_gbq(
      df_to_load,
      '{}_dataset'.format(instrument_type.lower()),
      '{}_quotes'.format(market.lower())
    )

    print('Total rows: {}'.format(rows))
    print('Total count: {}'.format(count))

  end = time.time()
  difference = int(end - start)

  print('{} hours to retrieve +20 years of historical data for {} {} symbols.'.format(difference / 3600, len(symbols), market.upper()))

# Running the script
if __name__ == '__main__':
  perform('NYSE', 'equity')
