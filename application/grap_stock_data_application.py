import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.resolve()))

from engine.realtime_engine_ibkr.stock_data_engine import *


def main():
    # if you want to change the output path, go to __init__ of ibkr_stock_data_io_engine to change it
    # now the default output path is dynamodb/ticker_data/one_min
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=1)
    engine = ibkr_stock_data_io_engine(ib)
    # engine.get_historical_data_by_range("AAPL", 1609430400, 1643594952, "1 min", False)
    # if the process is interrupted in the middle, keep the start timestamp and change the end timestamp in the "get_historical_data_by_range" to the earliest timestamp in the csv file -> run the program again
    data = engine.get_historical_currency_rate_by_range('USD', 'CNH', 1072929600, 1653451200)


if __name__ == "__main__":
    main()
