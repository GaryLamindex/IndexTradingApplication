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
    tickers = []
    initial_amount = 0
    check_ratio = False
    stock_data_engines = {}
    getdata = []
    timestamps = []
    rebalance_ratio = []
    quick_test = True

    def __init__(self, tickers, initial_amount, start_date, end_date, cal_stat, data_freq, user_id,
                 db_mode, quick_test, acceptance_range, rebalance_ratio):

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
        self.rebalance_ratio = rebalance_ratio
        for ticker in self.tickers:
            self.stock_data_engines[ticker] = local_engine(ticker, self.data_freq)
            self.getdata.append(False)

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
        # calculate all possible ratio that sum is 100 with different number of stickers
        num_tickers = len(self.tickers)
        print("Start Backtest:", self.rebalance_ratio)
        self.rebalance_dict = {}
        for ticker_num in range(num_tickers):
            self.rebalance_dict.update({self.tickers[ticker_num]: self.rebalance_ratio[ticker_num]})
        self.check_rebalance_ratio()
        if self.check_ratio:
            backtest_spec = self.rebalance_dict
            spec_str = ""
            for k, v in backtest_spec.items():
                spec_str = f"{spec_str}{str(v)}_{str(k)}_"

            acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                         self.table_name, spec_str)
            portfolio_data_engine = backtest_portfolio_data_engine(acc_data, self.tickers)
            trade_agent = backtest_trade_engine(acc_data, self.stock_data_engines, portfolio_data_engine)
            sim_agent = simulation_agent(self.rebalance_dict, self.table_info, False, portfolio_data_engine,
                                         self.tickers)

            algorithm = portfolio_rebalance(trade_agent, portfolio_data_engine, self.rebalance_dict,
                                            self.acceptance_range)
            self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, algorithm,
                               portfolio_data_engine, sim_agent)
            print("Finished Backtest:", backtest_spec)
            print("-------------------------------------------------------------------------------")
            self.plot_all_file_graph()

            if self.cal_stat:
                self.cal_all_file_return()

    def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm, portfolio_data_engine,
                      sim_agent):
        # connect to downloaded ib data to get price data
        row = 0

        for ticker_num in range(len(self.tickers)):
            if self.getdata[ticker_num] == False:
                self.timestamps.append(self.stock_data_engines[self.tickers[ticker_num]].
                                       get_data_by_range([start_timestamp, end_timestamp])['timestamp'])
                self.getdata[ticker_num] = True
        for ticker in self.timestamps:
            for timestamp in ticker:
                _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
                _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
                print('#' * 20, _date, ":", _time, '#' * 20)

                if row == 0:
                    # input initial cash
                    portfolio_data_engine.deposit_cash(initial_amount, timestamp)
                    row += 1

                if self.quick_test:
                    if algorithm.check_exec(timestamp, freq="Monthly", relative_delta=1):
                        self.run(timestamp, algorithm, sim_agent)
                else:
                    self.run(timestamp, algorithm, sim_agent)

    def check_rebalance_ratio(self):
        total_ratio = 0
        for k, v in self.rebalance_dict.items():
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
        stock_data_dict = {}
        sim_meta_data = {}
        for ticker in self.tickers:
            # get stock data from historical data
            ticker_data = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(timestamp)
            if ticker_data != None:
                ticker_open_price = ticker_data.get("open")
                stock_data_dict.update({ticker: {'last': ticker_open_price}})
                sim_meta_data.update({ticker: ticker_data})

            # ticker_div = ticker_stock_data.get(Query().timestamp == timestamp).get(ticker + ' div amount')
            # div_data_dict.update({ticker + ' div amount': ticker_div})

        orig_account_snapshot_dict = sim_agent.portfolio_data_engine.get_account_snapshot()
        # input database and historical data into algo
        action_msgs = algorithm.run(stock_data_dict, timestamp)

        sim_agent.append_run_data_to_db(timestamp, orig_account_snapshot_dict, action_msgs, sim_meta_data,
        stock_data_dict)

    @staticmethod
    def get_outcomes( dim, target):

        if dim == 2:
            outcomes = []
            for i in range(0, target + 1):
                outcomes.append([i, target - i])
            return outcomes
        else:
            result = []
            for i in range(0, target + 1):
                outcomes = backtest.get_outcomes(dim - 1, target - i)
                for j in outcomes:
                    j.append(i)
                    result.append(j)
            return result


def main():
    pass


if __name__ == "__main__":
    main()
