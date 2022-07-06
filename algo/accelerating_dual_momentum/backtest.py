import os
import pathlib
from datetime import datetime
import pandas as pd
from os import listdir
from pathlib import Path
from algo.accelerating_dual_momentum.algorithm import accelerating_dual_momentum
from engine.backtest_engine.dividend_engine import dividend_engine
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.simulation_engine import sim_data_io_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from engine.simulation_engine.statistic_engine import statistic_engine
# from engine.mongoDB_engine.write_document_engine import Write_Mongodb
from object.backtest_acc_data import backtest_acc_data
from engine.visualisation_engine import graph_plotting_engine
from object.action_data import IBAction, IBActionsTuple


class backtest:
    path = ""
    table_info = {}
    table_name = ""
    start_timestamp = 0
    end_timestamp = 0
    cal_stat = True
    data_freq = "one_min"
    db_mode = "local"
    tickers = []
    bond = ""
    initial_amount = 0
    stock_data_engines = {}

    def __init__(self, tickers, bond, initial_amount, start_date, end_date, cal_stat, data_freq, user_id,
                 db_mode):
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"

        self.table_info = {"mode": "backtest", "strategy_name": "accelerating_dual_momentum", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.tickers = tickers
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.end_timestamp = datetime.timestamp(end_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.db_mode = db_mode
        self.bond = bond
        for ticker in self.tickers:
            self.stock_data_engines[ticker] = local_engine(ticker, self.data_freq)
        self.stock_data_engines[self.bond] = local_engine(self.bond, self.data_freq)
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

    def loop_through_param(self):
        print("statr backtest")
        backtest_spec = self.tickers
        spec_str = f"{backtest_spec[0]}_{backtest_spec[1]}_"
        run_file = self.run_file_dir + spec_str + '.csv'
        if os.path.exists(run_file):
            os.remove(Path(run_file))
        graph_file = self.graph_dir + spec_str + '.png'
        if os.path.exists(graph_file):
            os.remove(Path(graph_file))

        acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                     self.table_name, spec_str)
        portfolio_data_engine = backtest_portfolio_data_engine(acc_data, self.tickers)
        trade_agent = backtest_trade_engine(acc_data, self.stock_data_engines, portfolio_data_engine)
        sim_agent = simulation_agent(backtest_spec, self.table_info, False, portfolio_data_engine,
                                     self.tickers)
        dividend_agent = dividend_engine(self.tickers)
        algorithm = accelerating_dual_momentum()
        self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, algorithm,
                           portfolio_data_engine, sim_agent, dividend_agent, trade_agent)
        print("Finished Backtest:", backtest_spec)
        print("-------------------------------------------------------------------------------")

        self.plot_all_file_graph()
        list_of_stats_data = listdir(self.stats_data_dir)
        for file in list_of_stats_data:
            os.remove(Path(f"{self.stats_data_dir}/{file}"))
        if self.cal_stat:
            self.cal_all_file_return()

    def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm,
                           portfolio_data_engine, sim_agent, dividend_agent, trade_agent):
        if len(self.tickers) != 2:
            print('This strategy only works for two tickers')
            exit(0)
        print('start backtest')
        print('Fetch data')
        portfolio_data_engine.deposit_cash(initial_amount, start_timestamp)
        series_1 = self.stock_data_engines[self.tickers[0]].get_data_by_range([start_timestamp, end_timestamp])[
            'timestamp']
        series_2 = self.stock_data_engines[self.tickers[1]].get_data_by_range([start_timestamp, end_timestamp])[
            'timestamp']
        timestamps = self.stock_data_engines[self.tickers[0]].get_union_timestamps(series_1, series_2)
        for timestamp in timestamps:
            _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, '#' * 20)
            
        

    def plot_all_file_graph(self):
        pass

    def cal_all_file_return(self):
        pass


