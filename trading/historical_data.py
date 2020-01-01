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

# Retrieves all ticker symbols of NYSE stocks.
def get_symbols():
  alphabet = list(string.ascii_uppercase)

  symbols = []

  print('Start parsing eoddata to retrieve symbols...')
  for each in alphabet:
    url = 'http://eoddata.com/stocklist/NYSE/{}.htm'.format(each)
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
    symbols_clean.append((each.split('-')[0]))

  print('{} NYSE ticker symbols retrieved and cleaned.'.format(str(len(symbols_clean))))

  return symbols_clean

# Retrieves +20 years for historical quotes for a single stock.
def retrieve(symbol):
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
      time.sleep(60)
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

      print('Time series retrieval succeeded!')
      break
  else:
    print('All {} attempts to retrieve data failed.'.format(attempts))

  return df

# Uploads data to BigQuery table.
def upload(dataframe, dataset_id, table_id):
  if dataframe.empty:
    print('No data to upload.')
    return

  res = None

  for attempt in range(attempts):
    try:
      client = bigquery.Client()

      dataset_ref = client.dataset(dataset_id)
      table_ref = dataset_ref.table(table_id)

      job_config = bigquery.LoadJobConfig()
      job_config.source_format = bigquery.SourceFormat.CSV
      job_config.autodetect = True
      job_config.ignore_unknown_values = True

      print('Start upload to BigQuery...')
      job = client.load_table_from_dataframe(
        dataframe,
        table_ref,
        location='EU',
        job_config=job_config
      )
    except Exception as e:
      print('Attempt {} to upload data for stock {} failed. Error: {}'.format(attempt + 1, dataframe['symbol'].iloc[0], e))
      time.sleep(10)
    else:
      print('Upload succeeded!')
      res = 'Upload succeeded'
      break
  else:
    res = 'Upload failed'
    print('All {} attemps to retrieve data for stock {} failed'.format(attempts, dataframe['symbol'].iloc[0]))

  return res

# Retrieves and uploads data for all NYSE stocks.
def perform(symbols):
  count = 0

  start = time.time()
  print('START OF HISTORICAL DATA RETRIEVAL')
  for symbol in symbols:
    print('------------------- Stock: {} -------------------'.format(symbol))
    historical_data = retrieve(symbol)

    upload(
      historical_data,
      'equity_data',
      'nyse_quotes'
    )

    count += 1
    print('Total count: {}'.format(count))

  end = time.time()
  difference = int(end - start)
  print('END OF HISTORICAL DATA RETRIEVAL')
  print('{} hours to retrieve +20 years of historical data for {} NYSE stocks.'.format(difference / 3600, len(symbols)))

symbols = get_symbols()
perform(symbols)
