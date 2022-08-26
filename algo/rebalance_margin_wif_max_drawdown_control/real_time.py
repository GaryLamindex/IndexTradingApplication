import time
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from datetime import datetime
from algo.rebalance_margin_wif_max_drawdown_control.backtest import backtest as max_drawdown_backtest
from object.action_data import IBAction, IBActionsTuple
from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.visualisation_engine import graph_plotting_engine
import os
import pathlib
from pathlib import Path
from engine.mongoDB_engine.write_run_data_document_engine import Write_Mongodb


class realtime:
    def __init__(self, tickers, initial_amount, start_date, end_date, rabalance_dict, maintain_dict,
                 max_drawdown_ratio_dict, purchase_exliq_ratio_dict, cal_stat, data_freq, user_id,
                 db_mode, quick_test,
                 store_mongoDB, strategy_initial='None', video_link='None', documents_link='None',
                 tags_array=[], subscribers_num=0, rating_dict={},
                 margin_ratio=np.NaN, trader_name='None'):
        self.end_date = end_date
        self.rabalance_dict = rabalance_dict
        self.maintain_dict = maintain_dict
        self.max_drawdown_ratio = max_drawdown_ratio_dict
        self.purchase_exliq_ratio_dict = purchase_exliq_ratio_dict
        self.quick_test = quick_test
        self.store_mongoDB = store_mongoDB
        self.strategy_initial = strategy_initial
        self.video_link = video_link
        self.documents_link = documents_link
        self.tags_array = tags_array
        self.subscribers_num = subscribers_num
        self.rating_dict = rating_dict
        self.margin_ratio = margin_ratio
        self.trader_name = trader_name
        self.stat_agent = None
        self.trader_name = None
        self.margin_ratio = None
        self.rating_dict = None
        self.subscribers_num = None
        self.tags_array = None
        self.documents_link = None
        self.video_link = None
        self.strategy_initial = None
        self.store_mongoDB = None
        self.stock_data_engines = {}
        self.algorithm = None
        self.dividend_agent = None
        self.sim_agent = None
        self.trade_agent = None
        self.portfolio_data_engine = None
        self.acc_data = None
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/realtime"

        self.table_info = {"mode": "realtime", "strategy_name": "margin_wif_max_drawdown_control", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.user_id = user_id
        self.tickers = tickers
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.db_mode = db_mode
        self.start_date = start_date
        self.backtest = None
        self.now = datetime.now()
        self.init_backtest_flag = False
        self.run_file_dir = f"{self.path}/{self.table_name}/run_data/"
        self.backtest_data_directory = os.fsencode(self.run_file_dir)
        if db_mode.get("local"):

            self.run_file_dir = f"{self.path}/{self.table_name}/run_data/"
            self.stats_data_dir = f"{self.path}/{self.table_name}/stats_data/"
            self.acc_data_dir = f"{self.path}/{self.table_name}/acc_data/"
            self.transact_data_dir = f"{self.path}/{self.table_name}/transaction_data/"
            self.graph_dir = f"{self.path}/{self.table_name}/graph"

            if not os.path.exists(self.run_file_dir):
                Path(self.run_file_dir).mkdir(parents=True, exist_ok=True)
            if not os.path.exists(self.stats_data_dir):
                Path(self.stats_data_dir).mkdir(parents=True, exist_ok=True)
            if not os.path.exists(self.acc_data_dir):
                Path(self.acc_data_dir).mkdir(parents=True, exist_ok=True)
            if not os.path.exists(self.transact_data_dir):
                Path(self.transact_data_dir).mkdir(parents=True, exist_ok=True)
            if not os.path.exists(self.graph_dir):
                Path(self.graph_dir).mkdir(parents=True, exist_ok=True)

