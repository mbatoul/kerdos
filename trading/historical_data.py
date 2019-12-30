import os
from dotenv import load_dotenv
import requests
import numpy as np
import pandas as pd
import string
import time
from alpha_vantage.timeseries import TimeSeries
from google.cloud import bigquery

load_dotenv()

alpha_vantage_api_key = os.environ.get('ALPHAVANTAGE_API_KEY')

# Loop over selected symbols
symbol = 'MC.PA'

# for symbol in symbols:
ts = TimeSeries(
  key=alpha_vantage_api_key,
  output_format='pandas'
)

df, metadata = ts.get_daily(
  symbol=symbol,
  outputsize='full'
)

# Clean column names (remove numbers)
for column in df.columns:
  df = df.rename({column: column[3:]}, axis='columns')

# Add symbol column
df.insert(loc=0, column='symbol', value=symbol)

client = bigquery.Client()

dataset_id = 'equity_data'
table_id = 'daily_quote_data'

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.autodetect = True
job_config.ignore_unknown_values = True
job = client.load_table_from_dataframe(
  df,
  table_ref,
  location='FR',
  job_config=job_config
)

job.result()
