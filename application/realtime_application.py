import multiprocessing
import pathlib
from datetime import datetime
import time
import pandas as pd
from algo.portfolio_rebalance.realtime import realtime as rebalance_realtime
from application.realtime_statistic_application import realtime_statistic
from engine.grab_data_engine.grab_data_engine import grab_stock_data_engine
from algo.accelerating_dual_momentum.realtime import realtime as accelerating_realtime
from application.lazyportfolioetf_scraper import lazyportfolioetf_engine as lazyportfolioetf
from algo.factor.realtime import Realtime as factor_realtime

def rebalance_process_function(tickers, rebalance_ratio, initial_amount, start_date, data_freq, user_id, cal_stat,
                               db_mode,
                               acceptance_range, execute_period):
    stock_engine = grab_stock_data_engine()
    realtime_backtest = rebalance_realtime(tickers, initial_amount, start_date, cal_stat,
                                           data_freq, user_id, db_mode, acceptance_range,
                                           rebalance_ratio, execute_period)
    ticker_name_path = stock_engine.ticker_name_path
    stock_engine.get_missing_daily_data()
    last_exec_timestamp = datetime.now()

    # while True:
    now_timestamp = datetime.now()
    if (now_timestamp - last_exec_timestamp).total_seconds() >= 10800:
        stock_engine.get_missing_daily_data()
        last_exec_timestamp = datetime.now()
    realtime_backtest.run()
    time.sleep(60)


def accelerating_process_function(tickers, bond, initial_amount, start_date, cal_stat, data_freq, user_id,
                                  db_mode, execute_period):
    stock_engine = grab_stock_data_engine()
    realtime_backtest = accelerating_realtime(tickers, bond, initial_amount, start_date, cal_stat, data_freq, user_id,
                                              db_mode, execute_period)
    ticker_name_path = stock_engine.ticker_name_path
    stock_engine.get_missing_daily_data()
    last_exec_timestamp = datetime.now()
    # while True:
    now_timestamp = datetime.now()
    if (now_timestamp - last_exec_timestamp).total_seconds() >= 10800:
        last_exec_timestamp = datetime.now()
        stock_engine.get_missing_daily_data()

    realtime_backtest.run()
    time.sleep(60)

def factor_process_function(tickers, initial_amount, start_date, cal_stat, data_freq, user_id,
                                  db_mode, execute_period):
    stock_engine = grab_stock_data_engine()
    realtime_backtest = factor_realtime(tickers, initial_amount, start_date, cal_stat, data_freq, user_id,
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

def stat_process_function(user_id, spec_str):
    time.sleep(300)
    realtime_stat = realtime_statistic(user_id, spec_str)
    realtime_stat.cal_stat_function()
    time.sleep(86400)


if __name__ == "__main__":
    portfolio_engine = lazyportfolioetf()
    portfolio_engine.get_portfolio()
    df = pd.read_csv(str(
        pathlib.Path(__file__).parent.parent.parent.resolve()) + "/etf_list/portfolio.csv")
    df['Weight'] = df['Weight'].str.rstrip('%').astype('float')
    df = df.loc[df['Weight'] != 100]
    df1 = df.groupby('Strategy Name')['Weight'].apply(list).reset_index(name='Weight')
    df2 = df.groupby('Strategy Name')['Ticker'].apply(list).reset_index(name='Ticker')
    rebalance_ratio = df1['Weight'].values.tolist()
    rebalance_tickers = df2['Ticker'].values.tolist()
    print(rebalance_ratio)
    print(rebalance_tickers)
    factor_tickers = [['SPY', 'QQQ', 'BND'],['VWO', 'GLD', 'GSG']]
    accelerating_tickers = [["BND", "BNDX"], ["DBC", "DES"]]
    initial_amount = 10000
    bond = "TIP"
    deposit_amount = 1000000
    start_date = datetime(2020, 1, 1)  # YYMMDD
    data_freq = "one_day"
    user_id = 0
    cal_stat = True
    db_mode = {"dynamo_db": False, "local": True}
    acceptance_range = 0
    execute_period = "Monthly"
    portfolio_rebalance = [None for i in range(len(rebalance_tickers))]
    for x in range(len(rebalance_tickers)):
        temp = multiprocessing.Process(target=rebalance_process_function,
                                       args=(rebalance_tickers[x], rebalance_ratio[x],
                                             initial_amount, start_date,
                                             data_freq, user_id, cal_stat,
                                             db_mode, acceptance_range,
                                             execute_period))
        temp.start()
        portfolio_rebalance[x] = temp
        temp.join()
    portfolio_stat = [None for i in range(len(rebalance_tickers))]
    for y in range(0, len(rebalance_tickers)):
        spec = ''
        for x in range(len(rebalance_tickers[y])):
            spec = spec + f"{int(rebalance_ratio[y][x])}_{rebalance_tickers[y][x]}_"
        temp = multiprocessing.Process(target=stat_process_function, args=(0, spec))
        temp.start()
        portfolio_stat[y] = temp
        temp.join()
    # SPY_GLD_BND_factor = multiprocessing.Process(target=factor_process_function,
    #                                              args=(factor_tickers[0], deposit_amount, start_date,
    #                                                    cal_stat, data_freq, user_id,
    #                                                    db_mode, execute_period))
    # VWO_QQQ_GSG_factor = multiprocessing.Process(target=factor_process_function,
    #                                              args=(factor_tickers[1], deposit_amount, start_date,
    #                                                    cal_stat, data_freq, user_id,
    #                                                    db_mode, execute_period))
    # SPY_GLD_BND_factor.start()
    # VWO_QQQ_GSG_factor.start()
    # SPY_GLD_BND_factor.join()
    # VWO_QQQ_GSG_factor.join()
    # for x in range(len(portfolio_rebalance)):
    #     portfolio_rebalance[x].start()
    # for x in range(len(portfolio_stat)):
    #     portfolio_stat[x].start()
    # for x in range(len(portfolio_stat)):
    #     portfolio_stat[x].join()
    # for x in range(len(portfolio_rebalance)):
    #     portfolio_rebalance[x].join()


    # BND_BNDX_accelerating = multiprocessing.Process(target=accelerating_process_function,
    #                                                 args=(accelerating_tickers[0], bond, deposit_amount, start_date,
    #                                                       cal_stat, data_freq, user_id,
    #                                                       db_mode, execute_period))
    # DBC_DES_accelerating = multiprocessing.Process(target=accelerating_process_function,
    #                                                args=(accelerating_tickers[1], bond, deposit_amount, start_date,
    #                                                      cal_stat, data_freq, user_id,
    #                                                      db_mode, execute_period))
    # SPY_MSFT_stat = multiprocessing.Process(target=stat_process_function, args=(0, "50_SPY_50_MSFT_"))
    # M_MSFT_stat = multiprocessing.Process(target=stat_process_function, args=(0, "50_M_50_MSFT_"))
    # M_MSFT_rebalance.start()
    # SPY_MSFT_rebalance.start()
    # BND_BNDX_accelerating.start()
    # DBC_DES_accelerating.start()

    # SPY_MSFT_stat.start()
    # M_MSFT_stat.start()
    # M_MSFT_stat.join()
    # SPY_MSFT_stat.join()
    # M_MSFT_rebalance.join()
    # SPY_MSFT_rebalance.join()
    # BND_BNDX_accelerating.join()
    # DBC_DES_accelerating.join()
