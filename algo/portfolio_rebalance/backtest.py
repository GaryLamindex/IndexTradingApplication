import os
import pathlib
from datetime import datetime
from os import listdir
from pathlib import Path
from algo.portfolio_rebalance.algorithm import portfolio_rebalance
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.simulation_engine import sim_data_io_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from engine.simulation_engine.statistic_engine import statistic_engine
from object.backtest_acc_data import backtest_acc_data
from engine.visualisation_engine import graph_plotting_engine


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
    rebalance_dict = {}
    tickers = {}
    initial_amount = 0
    check_ratio = False
    stock_data_engines = {}
    market_value = 0
    tickers_list = []
    def __init__(self, tickers, initial_amount, start_date, end_date, cal_stat, data_freq, user_id,
                 db_mode, quick_test, acceptance_range, market_value):
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"

        self.table_info = {"mode": "backtest", "strategy_name": "portfolio_rebalance", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.tickers = tickers
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.end_timestamp = datetime.timestamp(end_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.quick_test = quick_test
        self.db_mode = db_mode
        self.acceptance_range = acceptance_range
        self.market_value = market_value
        for ticker in self.tickers:
            self.stock_data_engines[ticker] = local_engine(ticker, self.data_freq)

        if db_mode.get("local"):

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
        possible_ratio = self.get_outcomes(len(self.tickers_list), 100)
        #calculate all possible ratio that sum is 100 with different number of stickers
        for ratio in possible_ratio:
            self.rebalance_dict = {}
            for ticker_num in range(len(self.tickers_list)):
                self.rebalance_dict.update({self.tickers_list[ticker_num]: ratio[ticker_num]})

                self.check_rebalance_ratio()
                if self.check_ratio:
                    backtest_spec = {}
                    for ticker, percentage in self.rebalance_dict:
                        rebalance_ratio = percentage / 100
                        backtest_spec.update({ticker: rebalance_ratio})
                    spec_str = ""
                    for k, v in backtest_spec.items():
                        spec_str = f"{spec_str}{str(v)}_{str(k)}_"

                    acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                                 self.table_name, spec_str)
                    portfolio_data_engine = backtest_portfolio_data_engine(acc_data, self.tickers)
                    trade_agent = backtest_trade_engine(acc_data, self.stock_data_engines, portfolio_data_engine)
                    sim_agent = simulation_agent(backtest_spec, self.table_info, False, portfolio_data_engine,
                                                 self.tickers)

                    algorithm = portfolio_rebalance(trade_agent, portfolio_data_engine, backtest_spec,
                                                    self.acceptance_range, self.market_value)
                    self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, algorithm,
                                       portfolio_data_engine, sim_agent)
                    print("Finished Backtest:", backtest_spec)
                    self.plot_all_file_graph()

                    if self.cal_stat:
                        print("start backtest")
                    self.cal_all_file_return()

    def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm, portfolio_data_engine,
                      sim_agent):
        # connect to downloaded ib data to get price data
        print("start backtest")
        row = 0
        print("Fetch data")
        timestamps = {}
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

            if self.quick_test:
                if algorithm.check_exec(timestamp, freq="Daily", relative_delta=1):
                    self.run(timestamp, algorithm, sim_agent)
            else:
                self.run(timestamp, algorithm, sim_agent)

    def check_rebalance_ratio(self):
        total_ratio = 0
        for k, v in self.rebalance_dict:
            ratio = v / 100
            total_ratio += ratio
        if total_ratio != 1:
            print("total ratio is not 100%")
            self.check_ratio = False
        else:
            self.check_ratio = True

    def plot_all_file_graph(self):
        print("plot_graph")
        graph_plotting_engine.plot_all_file_graph_png(f"{self.run_file_dir}", "date", "NetLiquidation",
                                                      f"{self.path}/{self.table_name}/graph")

    def cal_all_file_return(self):
        pass

    def run(self, timestamp, algorithm, sim_agent):
        pass



    def get_outcomes(self, dim, target):

        if dim == 2:
            outcomes = []
            for i in range(0, target + 1):
                outcomes.append([i, target - i])
            return outcomes
        else:
            result = []
            for i in range(0, target + 1):
                outcomes = self.get_outcomes(dim - 1, target - i)
                for j in outcomes:
                    j.append(i)
                    result.append(j)
            return result
def main(self):
    pass
if __name__ == "__main__":
    main()