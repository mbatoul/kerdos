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
from historical_data import get_symbols, load_to_gbq, retrieve_from_av, attempts

today = datetime.today().strftime('%Y-%m-%d')
print('Current day: {}'.format(today))
