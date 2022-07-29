import datetime as dt
import time

from ib_insync.ib import IB
from engine.grab_data_engine.grab_data_engine import *


def main():
    # Download missing data
    # Run indefinitely if True
    infinite_run = True
    # ib = IB()
    # ib.connect('127.0.0.1', 7497, clientId=1)
    stock_engine = grab_stock_data_engine()
    crypto_engine = grab_crypto_data_engine()

    last_exec_timestamp = None
    ticker_name_path = stock_engine.ticker_name_path
    ticker_data_path = stock_engine.daily_ticker_data_path
    tickers = pd.read_csv(f"{ticker_name_path}/ticker_name.csv")['Ticker']
    stock_engine.get_missing_daily_data()
    last_exec_timestamp = dt.datetime.now()
    # Download data indefinitely
    while infinite_run:
        now_timestamp = dt.datetime.now()
        if (now_timestamp - last_exec_timestamp).total_seconds() >= 43200:
            stock_engine.get_missing_daily_data()
            last_exec_timestamp = dt.datetime.now()
        time.sleep(3600)


if __name__ == "__main__":
    main()
