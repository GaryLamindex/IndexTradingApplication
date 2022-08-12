# import multiprocessing
# from datetime import datetime
import time
#
# from algo.portfolio_rebalance.realtime import realtime
# from application.realtime_statistic_application import realtime_statistic
# from engine.grab_data_engine.grab_data_engine import grab_stock_data_engine
#
#
# def process_function(tickers, rebalance_ratio, initial_amount, start_date, data_freq, user_id, cal_stat, db_mode,
#                      acceptance_range, execute_period):
#     stock_engine = grab_stock_data_engine()
#     realtime_backtest = realtime(tickers, initial_amount, start_date, cal_stat,
#                                  data_freq, user_id, db_mode, acceptance_range,
#                                  rebalance_ratio, execute_period)
#     ticker_name_path = stock_engine.ticker_name_path
#     stock_engine.get_missing_daily_data()
#     last_exec_timestamp = datetime.now()
#
#     while True:
#         now_timestamp = datetime.now()
#         if (now_timestamp - last_exec_timestamp).total_seconds() >= 43200:
#             stock_engine.get_missing_daily_data()
#             last_exec_timestamp = datetime.now()
#         realtime_backtest.run()
#         time.sleep(60)
#
#
# def stat_process_function(user_id, spec_str):
#     time.sleep(300)
#     realtime_stat = realtime_statistic(user_id, spec_str)
#     realtime_stat.cal_stat_function()
#     time.sleep(86400)
#
#
# if __name__ == "__main__":
#     tickers = [["M", "MSFT"], ["SPY", "MSFT"]]
#     rebalance_ratio = [[50, 50]]
#     initial_amount = 10000
#     start_date = datetime(2020, 1, 1)  # YYMMDD
#     data_freq = "one_day"
#     user_id = 0
#     cal_stat = True
#     db_mode = {"dynamo_db": False, "local": True}
#     acceptance_range = 0
#     execute_period = "Monthly"
#     M_MSFT = multiprocessing.Process(target=process_function, args=(tickers[0], rebalance_ratio[0],
#                                                                     initial_amount, start_date,
#                                                                     data_freq, user_id, cal_stat,
#                                                                     db_mode, acceptance_range,
#                                                                     execute_period))
#     SPY_MSFT = multiprocessing.Process(target=process_function, args=(tickers[1], rebalance_ratio[0],
#                                                                       initial_amount, start_date,
#                                                                       data_freq, user_id, cal_stat,
#                                                                       db_mode, acceptance_range,
#                                                                       execute_period))
#     SPY_MSFT_stat = multiprocessing.Process(target=stat_process_function, args=(0, "50_SPY_50_MSFT_"))
#     M_MSFT_stat = multiprocessing.Process(target=stat_process_function, args=(0, "50_M_50_MSFT_"))
#     M_MSFT.start()
#     SPY_MSFT.start()
#     SPY_MSFT_stat.start()
#     M_MSFT_stat.start()
#     M_MSFT_stat.join()
#     SPY_MSFT_stat.join()
#     M_MSFT.join()
#     SPY_MSFT.join()
from algo.accelerating_dual_momentum.realtime import realtime
from engine.grab_data_engine.grab_data_engine import grab_stock_data_engine
from datetime import datetime
import multiprocessing
def process_function(tickers, bond, initial_amount, start_date, cal_stat, data_freq, user_id,
                 db_mode,  execute_period):
    stock_engine = grab_stock_data_engine()
    realtime_backtest = realtime(tickers, bond, initial_amount, start_date, cal_stat, data_freq, user_id,
                                 db_mode, execute_period)
    ticker_name_path = stock_engine.ticker_name_path
    stock_engine.get_missing_daily_data()
    last_exec_timestamp = datetime.now()
    while True:
        now_timestamp = datetime.now()
        if (now_timestamp - last_exec_timestamp).total_seconds() >= 10800:
            last_exec_timestamp = datetime.now()
            stock_engine.get_missing_daily_data()

        realtime_backtest.run()
        time.sleep(60)

if __name__ == "__main__":
    tickers = [["BND", "BNDX"], ["DBC", "DES"]]
    bond = "TIP"
    deposit_amount = 1000000
    start_date = datetime(2020, 1, 1)
    strategy = "accelerating_dual_momentum"
    mode = "backtest"
    cal_stat = True
    quick_test = True
    wipe_previous_sim_data = True
    db_mode = {"dynamo_db": False, "local": True}
    data_freq = "one_day"
    user_id = 0
    execute_period = "Monthly"
    M_MSFT = multiprocessing.Process(target=process_function, args=(tickers[0], bond, deposit_amount, start_date,
                                                                    cal_stat, data_freq, user_id,
                                                                    db_mode,  execute_period))
    SPY_MSFT = multiprocessing.Process(target=process_function, args=(tickers[1], bond, deposit_amount, start_date,
                                                                    cal_stat, data_freq, user_id,
                                                                    db_mode,  execute_period))
    M_MSFT.start()
    SPY_MSFT.start()
    M_MSFT.join()
    SPY_MSFT.join()