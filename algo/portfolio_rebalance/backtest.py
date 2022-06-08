import os
import pathlib
from datetime import datetime
from os import listdir
from pathlib import Path
from algo.portfolio_rebalance.algorithm import portfolio_rebalance
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine

from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from object.backtest_acc_data import backtest_acc_data


class backtest(object):
    path = ""
    table_info = {}
    table_name = ""
    start_timestamp = 0
    end_timestamp = 0
    cal_stat = True
    data_freq = "one_min"
    db_mode = "local"
    acceptance_range = 0

    def __init__(self, tickers, initial_amount, start_date, end_date, cal_stat, rabalance_dict, data_freq, user_id, db_mode, quick_test, acceptance_range):
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"

        self.table_info = {"mode": "backtest", "strategy_name": "portfolio_rebalance","user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.tickers = tickers
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.end_timestamp = datetime.timestamp(end_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.quick_test = quick_test
        self.rabalance_dict = rabalance_dict
        self.db_mode = db_mode
        self.acceptance_range = acceptance_range

        for ticker in self.tickers:
            self.stock_data_engines[ticker] = local_engine(ticker, self.data_freq)

        if db_mode.get("local") == True:

            self.run_file_dir = f"{self.path}/{self.table_name}/run_data/"
            self.stats_data_dir = f"{self.path}/{self.table_name}/stats_data/"
            self.acc_data_dir = f"{self.path}/{self.table_name}/acc_data/"
            self.transact_data_dir = f"{self.path}/{self.table_name}/transaction_data/"
            self.graph_dir = f"{self.path}/{self.table_name}/graph"

            list_of_run_files = listdir(self.run_file_dir)
            list_of_stats_data = listdir(self.stats_data_dir)
            list_of_acc_data = listdir(self.acc_data_dir)
            list_of_transact_data = listdir(self.transact_data_dir)
            list_of_graph = listdir(self.graph_dir)

            for file in list_of_run_files:
                os.remove(Path(f"{self.run_file_dir}/{file}"))
            for file in list_of_stats_data:
                os.remove(Path(f"{self.stats_data_dir}/{file}"))
            for file in list_of_acc_data:
                os.remove(Path(f"{self.acc_data_dir}/{file}"))
            for file in list_of_transact_data:
                os.remove(Path(f"{self.transact_data_dir}/{file}"))
            for file in list_of_graph:
                os.remove(Path(f"{self.graph_dir}/{file}"))

    def loop_through_param(self):

        # loop through all the rebalance requirement
        for rebalance in range(rebalance_start, rebalance_end, rebalance_step):

            rebalance_ratio = rebalance / 1000

            backtest_spec = {"rebalance_margin": rebalance_margin, "max_drawdown_ratio": max_drawdown_ratio,
                             "purchase_exliq": purchase_exliq}
            spec_str = ""
            for k, v in backtest_spec.items():
                spec_str = f"{spec_str}{str(v)}_{str(k)}_"

            acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                         self.table_name, spec_str)
            portfolio_data_engine = backtest_portfolio_data_engine(acc_data, self.tickers)
            trade_agent = backtest_trade_engine(acc_data, self.stock_data_engines, portfolio_data_engine)
            sim_agent = simulation_agent(backtest_spec, self.table_info, False, portfolio_data_engine,
                                         self.tickers)

            algorithm = portfolio_rebalance(trade_agent, portfolio_data_engine, self.tickers,
                                                          max_drawdown_ratio, self.acceptance_range,
                                                          rebalance_margin)
            self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, algorithm,
                               portfolio_data_engine, sim_agent)
            print("Finished Backtest:", backtest_spec)
            self.plot_all_file_graph()

            if self.cal_stat == True:
                print("start backtest")
            self.cal_all_file_return()
    }


    def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm, portfolio_data_engine,sim_agent):
        # connect to downloaded ib data to get price data
        print("start backtest")
        row = 0
        print("Fetch data")

        if len(self.tickers) == 1:
            timestamps = self.stock_data_engines[self.tickers[0]].get_data_by_range([start_timestamp, end_timestamp])[
                'timestamp']
        elif len(self.tickers) == 2:
            series_1 = self.stock_data_engines[self.tickers[0]].get_data_by_range([start_timestamp, end_timestamp])[
                'timestamp']
            series_2 = self.stock_data_engines[self.tickers[1]].get_data_by_range([start_timestamp, end_timestamp])[
                'timestamp']
            timestamps = self.stock_data_engines[self.tickers[0]].get_union_timestamps(series_1, series_2)

        for timestamp in timestamps:
            _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, '#' * 20)

            if row == 0:
                # input initial cash
                portfolio_data_engine.deposit_cash(initial_amount, timestamp)
                row += 1

            if self.quick_test == True:
                if algorithm.check_exec(timestamp, freq="Daily", relative_delta=1):
                    self.run(timestamp, algorithm, sim_agent)
            else:
                self.run(timestamp, algorithm, sim_agent)

    def check_exec(self):
        boo= True

        return boo

def main():
    tickers = ["SPY", "GOVT"]
    ticker_dict = {"SPY": x%, "GOVT":y%, "3188":z%}
    loop_through_param(ticker_dict)
