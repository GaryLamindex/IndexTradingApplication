import datetime as dt
import time

from ib_insync.ib import IB
from engine.grab_data_engine.grab_data_engine import *


def main():
    # Download missing data
    # Run indefinitely if True
    infinite_run = True
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=1)
    stock_engine = grab_stock_data_engine(ib_instance=ib)
    crypto_engine = grab_crypto_data_engine()

    last_exec_timestamp = None
    ticker_name_path = stock_engine.ticker_name_path
    ticker_data_path = stock_engine.daily_ticker_data_path
    tickers = pd.read_csv(f"{ticker_name_path}/ticker_name.csv")['Ticker']
    stock_engine.get_missing_daily_data()
    last_exec_timestamp = dt.datetime.now()
    # Download data indefinitely
    while infinite_run:
        # Download daily stock data
        now_timestamp = dt.datetime.now()
        if (now_timestamp - last_exec_timestamp).total_seconds() >= 86400:
            last_exec_timestamp = dt.datetime.now()
            stock_engine.get_missing_daily_data()
        # Download per minute stock data
        stock_engine.get_multiple_min_data_by_range(0, dt.datetime.now().timestamp(), '1 min', False, tickers=None)


if __name__ == "__main__":
    main()
