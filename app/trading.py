import os
import sys
import json
sys.path.append('./lib/python3.7/site-packages')
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv
import functools
from flask import (
  Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from app.db import get_db

load_dotenv()

trading = Blueprint('trading', __name__, url_prefix='/trading', static_folder='static', template_folder='templates')

@trading.route('/index', methods=['GET'])
def index():
  api = tradeapi.REST(
    os.environ.get('APCA_API_KEY_ID'),
    os.environ.get('APCA_API_SECRET_KEY'),
    api_version='v2'
  )

  current_positions = api.list_positions()
  clock = api.get_clock()
  account = api.get_account()
  print(account)
  print(type(account))
  balance_change = float(account.equity) - float(account.last_equity)

  return render_template('trading/index.html',
    current_positions=current_positions,
    balance_change=balance_change,
    clock=clock,
    account=account
  )

@trading.route('/news', methods=['GET'])
def news():
  return render_template('trading/news.html')
