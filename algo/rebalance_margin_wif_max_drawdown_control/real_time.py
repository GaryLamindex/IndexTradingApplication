import os
from os import listdir
from pathlib import Path

from ibkr_engine.ibkr_portfolio_data_agent import ibkr_portfolio_data_agent
from rebalance_margin_wif_max_drawdown_control.algo import Rebalance_Margin_Wif_Max_Drawdown_Control


class real_time(object):
    path = ""
    db_path = ""
    data_freq = ""
    tickers = []
    stock_agent = None
    initial_amount = 0
    start_date = None
    end_date = None
    cal_stat = True
    wipe_previous_sim_data = False
    re_query_ib_data = False
    rabalance = 0
    maintain = 0
    max_drawdown_ratio = 0
    purchase_exliq_ratio = 0
    algo = None

    def __init__(self, tickers, cal_stat, rebalance_margin, maintain_margin, max_drawdown_ratio, purchase_exliq, data_freq, wipe_previous_sim_data):
        self.path = "rebalance_margin_wif_max_drawdown_control/real_time"
        self.db_path = self.path+ "/db"
        self.tickers = tickers

        self.cal_stat = cal_stat
        self.data_freq = data_freq

        self.rebalance_margin = rebalance_margin
        self.maintain_margin = maintain_margin
        self.max_drawdown_ratio = max_drawdown_ratio
        self.purchase_exliq = purchase_exliq


    def exec(self):

        # # init stock data and query historical data
        # stock_data = TinyDB(self.db_path+"/stock_data.json")
        # stock_data.truncate()
        # stock_agent = StockData(self.db_path)
        # stock_agent.query_his_data(self.tickers, self.start_date, self.end_date, 'DAILY')

        # delete all the previous similation data
        _sim_data_db = ibkr_portfolio_data_agent(self.path)
        if self.wipe_previous_sim_data == True:
            _sim_data_db.init_data()

        # init portforlio database

        sim_data_db_name = str(self.rebalance_margin) + "_rebalance_margin_" + str(
            self.maintain_margin) + "_maintain_margin_" + str(
            self.max_drawdown_ratio) + "max_drawdown_" + "_purchase_exliq_" + str(self.purchase_exliq)
        algo = Rebalance_Margin_Wif_Max_Drawdown_Control(self.path, self.tickers, self.rebalance_margin,
                                                         self.maintain_margin,
                                                         self.max_drawdown_ratio, self.purchase_exliq, sim_data_db_name,
                                                         "real_time")
        algo.real_time()

        if (self.cal_stat == True):
            self.cal_all_file_return()
            self.cal_all_file_month_to_month_return()
            self.cal_parameter_start_year_return_stats()

