import numpy as np
import pandas as pd
import yfinance as yf
import pathlib
from datetime import datetime
from algo.portfolio_rebalance.backtest import backtest as portfolio_rebalance_backtest

class realtime:
    def __init__(self, list_of_tickers, initial_amount, start_date, cal_stat, data_freq, user_id,
                 db_mode, acceptance_range, list_of_rebalance_ratios, store_mongoDB, strategy_initial='None',
                 video_link='None', documents_link='None', tags_array=list(), subscribers_num=0,
                 rating_dict={}, margin_ratio=np.NaN, trader_name='None'):
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

    def init_backtest(self, user_id, acceptance_range):
        self.now = datetime.now()

        end_date = self.now.date()
        self.backtest = portfolio_rebalance_backtest(self.list_of_tickers, self.initial_amount,
                                                     self.start_date, end_date, True, self.data_freq,
                                                     user_id, self.db_mode, True, acceptance_range, )
        pass


def main():
    data = yf.download(tickers='MSFT', period='1m', interval='1m')
    now = datetime.today()
    a = 0


if __name__ == "__main__":
    main()
