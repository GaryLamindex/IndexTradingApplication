import multiprocessing
from datetime import datetime
import time

from algo.portfolio_rebalance.realtime import realtime


def process_function(tickers, rebalance_ratio, initial_amount, start_date, data_freq, user_id, cal_stat, db_mode,
                     acceptance_range, execute_period):
    realtime_backtest = realtime(tickers, initial_amount, start_date, cal_stat,
                                 data_freq, user_id, db_mode, acceptance_range,
                                 rebalance_ratio, execute_period)
    while True:
        realtime_backtest.run()
        time.sleep(60)


if __name__ == "__main__":
    tickers = [["M", "MSFT"], ["SPY", "MSFT"]]
    rebalance_ratio = [[50, 50]]
    initial_amount = 10000
    start_date = datetime(2020, 1, 1)  # YYMMDD
    data_freq = "one_day"
    user_id = 0
    cal_stat = True
    db_mode = {"dynamo_db": False, "local": True}
    acceptance_range = 0
    execute_period = "Monthly"
    M_MSFT = multiprocessing.Process(target=process_function, args=(tickers[0], rebalance_ratio[0],
                                                                    initial_amount, start_date,
                                                                    data_freq, user_id, cal_stat,
                                                                    db_mode, acceptance_range,
                                                                    execute_period))
    SPY_MSFT = multiprocessing.Process(target=process_function, args=(tickers[1], rebalance_ratio[0],
                                                                      initial_amount, start_date,
                                                                      data_freq, user_id, cal_stat,
                                                                      db_mode, acceptance_range,
                                                                      execute_period))
    M_MSFT.start()
    SPY_MSFT.start()
    M_MSFT.join()
    SPY_MSFT.join()