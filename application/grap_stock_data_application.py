import requests

from engine.realtime_engine_ibkr.stock_data_engine import *
from engine.backtest_engine.currencies_engine import *

sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.resolve()))


def main():
    # if you want to change the output path, go to __init__ of ibkr_stock_data_io_engine to change it
    # now the default output path is dynamodb/ticker_data/one_min
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=1)
    engine = ibkr_stock_data_io_engine(ib)

    # The following statement won't work all the time
    # if the process is interrupted in the middle,
    # keep the start timestamp and change the end timestamp in the "get_historical_data_by_range"
    # to the earliest timestamp in the csv file -> run the program again

    # The following two lines are fetching stock/forex data
    engine.get_historical_data_by_range("QQQ",
                                        dt.datetime(year=2004, month=3, day=10, tzinfo=dt.timezone.utc).timestamp(),
                                        dt.datetime(year=2004, month=7, day=1, tzinfo=dt.timezone.utc).timestamp(),
                                        "1 min", False)
    # engine.get_historical_currency_rate_by_range('USD', 'HKD', 946702800, 1590700500)
    # engine.get_sehk_historical_data_by_range('3188', 1351215000, 1351216020, '1 min', True)


if __name__ == "__main__":
    main()
