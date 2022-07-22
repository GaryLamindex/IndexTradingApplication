import numpy as np
import pandas as pd
import yfinance as yf
import pathlib
from datetime import datetime
from algo.portfolio_rebalance.backtest import backtest as portfolio_rebalance_backtest


class realtime:
    def __init__(self, list_of_tickers, initial_amount, start_date, cal_stat, data_freq, user_id,
                 db_mode, acceptance_range, list_of_rebalance_ratios):
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/realtime"

        self.table_info = {"mode": "backtest", "strategy_name": "portfolio_rebalance", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.user_id = user_id
        self.list_of_tickers = list_of_tickers
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.db_mode = db_mode
        self.rebalance_ratio = list_of_rebalance_ratios
        self.start_date = start_date
        self.tickers = []
        self.backtest = None
        self.now = datetime.now()
        self.acceptance_range = acceptance_range

    def init_backtest(self, user_id, acceptance_range, store_mongoDB, strategy_initial, video_link,
                      documents_link, tags_array, subscribers_num,
                      rating_dict, margin_ratio, trader_name):
        self.now = datetime.now()

        end_date = self.now.date()
        self.backtest = portfolio_rebalance_backtest(self.list_of_tickers, self.initial_amount,
                                                     self.start_date, end_date, True, self.data_freq,
                                                     user_id, self.db_mode, True, acceptance_range,
                                                     self.rebalance_ratio, store_mongoDB, strategy_initial,
                                                     video_link, documents_link, tags_array, subscribers_num,
                                                     rating_dict, margin_ratio, trader_name)
        pass

    def check_exec(self, timestamp, **kwargs): # check whether should do any execution in a specify period
        pass

    def run(self):  # decide run realtime or backtest

        pass

    def run_backtest(self): #run backtest
        pass

    def run_realtime(self): #run realtime
        pass


def main():
    tickers = ["M", "MSFT"]
    rebalance_ratio = [[50, 50]]
    initial_amount = 10000
    start_date = datetime(2010, 1, 1)  # YYMMDD
    data_freq = "one_day"
    user_id = 0
    cal_stat = True
    db_mode = {"dynamo_db": False, "local": True}
    acceptance_range = 0
    execute_period = "Monthly"

    realtime_backtest = realtime(tickers, initial_amount, start_date, cal_stat,
                                 data_freq, user_id, db_mode, acceptance_range,
                                 rebalance_ratio, )
    realtime_backtest.init_backtest(realtime_backtest.user_id, realtime_backtest.acceptance_range,
                                    store_mongoDB=True,
                                    strategy_initial='this is 20 80 m and msft portfolio',
                                    video_link='https://www.youtube.com',
                                    documents_link='https://google.com',
                                    tags_array=None,
                                    subscribers_num=3,
                                    rating_dict=None,
                                    margin_ratio=3.24,
                                    trader_name='Fai'
                                    )
    while True:
        if realtime_backtest.check_exec(execute_period):
            realtime_backtest.run()



if __name__ == "__main__":
    main()
