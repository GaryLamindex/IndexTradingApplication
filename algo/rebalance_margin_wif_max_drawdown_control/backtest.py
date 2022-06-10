import os
import pathlib
from datetime import datetime
from os import listdir
from pathlib import Path

import pandas as pd
from pandas.core import series

from algo.rebalance_margin_wif_max_drawdown_control.algorithm import rebalance_margin_wif_max_drawdown
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.simulation_engine import sim_data_io_engine
from engine.aws_engine.dynamo_db_engine import dynamo_db_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from engine.simulation_engine.statistic_engine import statistic_engine
from engine.simulation_engine.statistic_engine_2 import statistic_engine_2
from engine.simulation_engine.statistic_engine_3 import statistic_engine_3
from object.backtest_acc_data import backtest_acc_data

from engine.visualisation_engine import graph_plotting_engine


def run():
    pass


class backtest(object):
    path = ""
    data_freq = ""
    tickers = []
    initial_amount = 0
    start_timestamp = None
    end_timestamp = None
    cal_stat = True
    wipe_previous_sim_data = False
    quick_test = False
    rabalance_dict = {}
    maintain_dict = {}
    max_drawdown_ratio_dict = {}
    purchase_exliq_ratio_dict = {}
    algo = None
    dynamo_db = None
    table_info = {}
    table_name = ""

    run_file_dir = ""
    stats_data_dir = ""
    acc_data_dir = ""
    transact_data_dir = ""
    graph_dir = ""

    stock_data_engines = {}

    # maximum ONLY 2 tickers at a time !!!
    def __init__(self, tickers, initial_amount, start_date, end_date, cal_stat, rabalance_dict, maintain_dict,
                 max_drawdown_ratio_dict, purchase_exliq_ratio_dict, data_freq, user_id, db_mode, quick_test):
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"

        self.table_info = {"mode": "backtest", "strategy_name": "rebalance_margin_wif_max_drawdown_control",
                           "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.tickers = tickers
        # self.marketCol = f'{self.tickers[0]} marketPrice'
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.end_timestamp = datetime.timestamp(end_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.quick_test = quick_test
        self.rabalance_dict = rabalance_dict
        self.maintain_dict = maintain_dict
        self.max_drawdown_ratio_dict = max_drawdown_ratio_dict
        self.purchase_exliq_ratio_dict = purchase_exliq_ratio_dict

        for ticker in self.tickers:
            self.stock_data_engines[ticker] = local_engine(ticker, "one_min")

        if db_mode.get("dynamo_db") == True:
            dynamo_db = dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
            dynamo_db.init_table(self.table_name)
            dynamo_db.init_table("strategy_stats_data", user_id)
            ticket_data = dynamo_db.init_table("ticket_data")
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

        # # init stock data and query historical data
        # stock_data = TinyDB(self.db_path+"/stock_data.json")
        # stock_data.truncate()
        # stock_agent = StockData(self.db_path)
        # stock_agent.query_his_data(self.tickers, self.start_date, self.end_date, 'DAILY')

        # get rebalance requirement
        rebalance_start = self.rabalance_dict.get("start")
        rebalance_end = self.rabalance_dict.get("end")
        rebalance_step = self.rabalance_dict.get("step")

        maintain_start = self.maintain_dict.get("start")
        maintain_end = self.maintain_dict.get("end")
        maintain_step = self.maintain_dict.get("step")

        max_drawdown_start = self.max_drawdown_ratio_dict.get("start")
        max_drawdown_end = self.max_drawdown_ratio_dict.get("end")
        max_drawdown_step = self.max_drawdown_ratio_dict.get("step")

        purchase_exliq_ratio_start = self.purchase_exliq_ratio_dict.get("start")
        purchase_exliq_ratio_end = self.purchase_exliq_ratio_dict.get("end")
        purchase_exliq_ratio_step = self.purchase_exliq_ratio_dict.get("step")

        # loop through all the rebalance requirement
        for rebalance in range(rebalance_start, rebalance_end, rebalance_step):
            # for maintain in range(maintain_start, maintain_end, maintain_step):
            for max_drawdown in range(max_drawdown_start, max_drawdown_end, max_drawdown_step):
                for purchase_exliq_ratio in range(purchase_exliq_ratio_start, purchase_exliq_ratio_end,
                                                  purchase_exliq_ratio_step):

                    # if (maintain > rebalance):
                    #     continue

                    rebalance_margin = rebalance / 1000
                    # maintain_margin = maintain/1000
                    # maintain_margin = rebalance_margin
                    max_drawdown_ratio = max_drawdown / 1000
                    purchase_exliq = purchase_exliq_ratio / 100
                    acceptance_range = 0

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

                    algorithm = rebalance_margin_wif_max_drawdown(trade_agent, portfolio_data_engine, self.tickers,
                                                                  max_drawdown_ratio, acceptance_range,
                                                                  rebalance_margin)
                    self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, algorithm,
                                       portfolio_data_engine, sim_agent)
                    print("Finished Backtest:", backtest_spec)
                    self.plot_all_file_graph()

                    if self.cal_stat == True:
                        print("start backtest")
                        self.cal_all_file_return()

    def plot_all_file_graph(self):
        print("plot_graph")
        graph_plotting_engine.plot_all_file_graph_png(f"{self.run_file_dir}", "date", "NetLiquidation",
                                                      f"{self.path}/{self.table_name}/graph")

    def cal_all_file_return(self):
        sim_data_offline_engine = sim_data_io_engine.offline_engine(self.run_file_dir)
        backtest_data_directory = os.fsencode(self.run_file_dir)
        data_list = []
        for idx, file in enumerate(os.listdir(backtest_data_directory)):
            if file.decode().endswith("csv"):
                marketCol = f'{self.tickers[idx]} marketPrice'
                costCol = f'{self.tickers[idx]} costBasis'
                valueCol = f'{self.tickers[idx]} marketValue'
                file_name = file.decode().split(".csv")[0]
                stat_engine = statistic_engine(sim_data_offline_engine)
                stat_engine_2 = statistic_engine_2(sim_data_offline_engine)
                stat_engine_3 = statistic_engine_3(sim_data_offline_engine)
                sharpe_dict = stat_engine.get_sharpe_data(file_name)
                inception_sharpe = sharpe_dict.get("inception")
                _1_yr_sharpe = sharpe_dict.get("1y")
                _3_yr_sharpe = sharpe_dict.get("3y")
                _5_yr_sharpe = sharpe_dict.get("5y")
                _ytd_sharpe = sharpe_dict.get("ytd")

                sortino_dict = stat_engine.get_sortino_data(file_name)
                inception_sortino = sortino_dict.get('inception')
                _1_yr_sortino = sortino_dict.get('1y')
                _3_yr_sortino = sortino_dict.get('3y')
                _5_yr_sortino = sortino_dict.get('5y')
                _ytd_sortino = sortino_dict.get('ytd')

                return_dict = stat_engine.get_return_data(file_name)
                inception_return = return_dict.get("inception")
                _1_yr_return = return_dict.get("1y")
                _3_yr_return = return_dict.get("3y")
                _5_yr_return = return_dict.get("5y")
                _ytd_return = return_dict.get("ytd")

                max_drawdown_dict = stat_engine.get_max_drawdown_data(file_name)
                inception_max_drawdown = max_drawdown_dict.get("inception")
                _1_yr_max_drawdown = max_drawdown_dict.get("1y")
                _3_yr_max_drawdown = max_drawdown_dict.get("3y")
                _5_yr_max_drawdown = max_drawdown_dict.get("5y")
                _ytd_max_drawdown = max_drawdown_dict.get("ytd")

                alpha_dict = stat_engine.get_alpha_data(file_name, marketCol)
                inception_alpha = alpha_dict.get('inception')
                _1_yr_alpha = alpha_dict.get('1y')
                _3_yr_alpha = alpha_dict.get('3y')
                _5_yr_alpha = alpha_dict.get('5y')
                _ytd_alpha = alpha_dict.get('ytd')

                volatility_dict = stat_engine.get_volatility_data(file_name, marketCol)
                inception_volatility = volatility_dict.get('inception')
                _1_yr_volatility = volatility_dict.get('1y')
                _3_yr_volatility = volatility_dict.get('3y')
                _5_yr_volatility = volatility_dict.get('5y')
                _ytd_volatility = volatility_dict.get('ytd')

                win_rate_dict = stat_engine_2.get_win_rate_data(file_name)
                inception_win_rate = win_rate_dict.get('inception')
                _1_yr_win_rate = win_rate_dict.get('1y')
                _3_yr_win_rate = win_rate_dict.get('3y')
                _5_yr_win_rate = win_rate_dict.get('5y')
                _ytd_win_rate = win_rate_dict.get('ytd')

                dateStringS = datetime.fromtimestamp(self.start_timestamp)
                dateStringE = datetime.fromtimestamp(self.end_timestamp)
                date_range = [f"{dateStringS.year}-{dateStringS.month}-{dateStringS.day}", \
                              f"{dateStringE.year}-{dateStringE.month}-{dateStringE.day}"]
                rolling_return_dict = stat_engine.get_rolling_return_data(file_name, date_range)
                _1_yr_rolling_return = rolling_return_dict.get('1y')
                _2_yr_rolling_return = rolling_return_dict.get('2y')
                _3_yr_rolling_return = rolling_return_dict.get('3y')
                _5_yr_rolling_return = rolling_return_dict.get('5y')
                _7_yr_rolling_return = rolling_return_dict.get('7y')
                _10_yr_rolling_return = rolling_return_dict.get('10y')
                _15_yr_rolling_return = rolling_return_dict.get('15y')
                _20_yr_rolling_return = rolling_return_dict.get('20y')

                drawdown_dict = stat_engine.get_drawdown_data(file_name, date_range)

                all_file_stats_row = {
                    "Backtest Spec": file_name, 'YTD Return': _ytd_return, '1 Yr Return': _1_yr_return,
                    "3 Yr Return": _3_yr_return, "5 Yr Return": _5_yr_return,
                    "Since Inception Return": inception_return, "Since Inception Sharpe": inception_sharpe,
                    "YTD Sharpe": _ytd_sharpe,
                    "1 Yr Sharpe": _1_yr_sharpe, "3 Yr Sharpe": _3_yr_sharpe, "5 Yr Sharpe": _5_yr_sharpe,
                    'Since Inception Sortino': inception_sortino, 'YTD Sortino': _ytd_sortino,
                    '1 Yr Sortino': _1_yr_sortino, '3 Yr Sortino': _3_yr_sortino, '5 Yr Sortino': _5_yr_sortino,
                    "Since Inception Max Drawdown": inception_max_drawdown, "YTD Max Drawdown": _ytd_max_drawdown,
                    "1 Yr Max Drawdown": _1_yr_max_drawdown, "3 Yr Max Drawdown": _3_yr_max_drawdown,
                    "5 Yr Max Drawdown": _5_yr_max_drawdown,
                    "Since Inception Alpha": inception_alpha, "YTD Alpha": _ytd_alpha,
                    "1 Yr Alpha": _1_yr_alpha, "3 Yr Alpha": _3_yr_alpha,
                    "5 Yr Alpha": _5_yr_alpha,
                    "Since Inception Volatility": inception_volatility, "YTD Volatility": _ytd_volatility,
                    "1 Yr Volatility": _1_yr_volatility, "3 Yr Volatility": _3_yr_volatility,
                    "5 Yr Volatility": _5_yr_volatility,
                    "Since Inception Win Rate": inception_win_rate, "YTD Win Rate": _ytd_win_rate,
                    "1 Yr Win Rate": _1_yr_win_rate, "3 Yr Win Rate": _3_yr_win_rate,
                    "5 Yr Win Rate": _5_yr_win_rate,

                    "1 Yr Rolling Return": _1_yr_rolling_return, "2 Yr Rolling Return": _2_yr_rolling_return,
                    "3 Yr Rolling Return": _3_yr_rolling_return, "5 Yr Rolling Return": _5_yr_rolling_return,
                    "7 Yr Rolling Return": _7_yr_rolling_return, "10 Yr Rolling Return": _10_yr_rolling_return,
                    "15 Yr Rolling Return": _15_yr_rolling_return, "20 Yr Rolling Return": _20_yr_rolling_return,
                    "Drawdowns": drawdown_dict
                }
                # _additional_data = self.cal_additional_data(file_name)
                # data_list.append(all_file_stats_row | _additional_data)
                _additional_data = {}
                data_list.append(all_file_stats_row | _additional_data)

        col = ['Backtest Spec', 'YTD Return', '1 Yr Return', "3 Yr Return", "5 Yr Return",
               "Since Inception Return", "Since Inception Sharpe", "YTD Sharpe", "1 Yr Sharpe", "3 Yr Sharpe",
               "5 Yr Sharpe", 'Since Inception Sortino', 'YTD Sortino', '1 Yr Sortino', '3 Yr Sortino', '5 Yr Sortino',
               "Since Inception Max Drawdown", "YTD Max Drawdown",
               "1 Yr Max Drawdown",
               "3 Yr Max Drawdown", "5 Yr Max Drawdown",
               "Since Inception Alpha", "YTD Alpha", "1 Yr Alpha", "3 Yr Alpha", "5 Yr Alpha",
               "Since Inception Volatility", "YTD Volatility", "1 Yr Volatility", "3 Yr Volatility", "5 Yr Volatility",
               "Since Inception Win Rate", "YTD Win Rate", "1 Yr Win Rate", "3 Yr Win Rate", "5 Yr Win Rate",
               "1 Yr Rolling Return", "2 Yr Rolling Return", "3 Yr Rolling Return", "5 Yr Rolling Return",
               "7 Yr Rolling Return", "10 Yr Rolling Return", "15 Yr Rolling Return", "20 Yr Rolling Return",
               "Drawdown"]

        df = pd.DataFrame(data_list, columns=col)
        df.fillna(0)
        print(f"{self.path}/stats_data/{self.table_name}.csv")
        df.to_csv(f"{self.path}/{self.table_name}/stats_data/all_file_return.csv")
        pass

    #
    # def cal_all_file_month_to_month_return(self):
    #     stats_agent = statistic_agent(self.db_path)
    #     list_of_files = listdir(self.sim_data_path + "/csv")
    #
    #     for file in list_of_files:
    #         _data = stats_agent.cal_month_to_month_breakdown(file)
    #     pass

    def cal_additional_data(self, file_name):
        file_path = f"{self.run_file_dir}/{file_name}/.csv"
        df = pd.read_csv(file_path, low_memory=False)
        _exmk = df['ExcessLiquidity/ GrossPositionValue(Day End)'].min()
        _additional_data = {"min(ExcessLiquidity/ GrossPositionValue(Day End))": _exmk}
        return _additional_data

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

    def run(self, timestamp, algorithm, sim_agent):
        stock_data_dict = {}
        for ticker in self.tickers:
            # get stock data from historical data
            print("timestamp:", timestamp, "; ticker:", ticker)
            ticker_data = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(timestamp)
            if ticker_data != None:
                ticker_open_price = ticker_data.get("open")
                print("ticker_open_price", ticker_open_price)
                stock_data_dict.update({ticker: {'last': ticker_open_price}})
                print("stock_data_dict", stock_data_dict)

            # ticker_div = ticker_stock_data.get(Query().timestamp == timestamp).get(ticker + ' div amount')
            # div_data_dict.update({ticker + ' div amount': ticker_div})

        orig_account_snapshot_dict = sim_agent.portfolio_data_engine.get_account_snapshot()
        # input database and historical data into algo
        action_msgs = algorithm.run(stock_data_dict, timestamp)
        sim_meta_data = {}

        for ticker in self.tickers:
            sim_meta_data.update({ticker: {"max_stock_price": algorithm.max_stock_price[ticker]}})
            sim_meta_data[ticker]["benchmark_drawdown_price"] = algorithm.benchmark_drawdown_price[ticker]
            sim_meta_data[ticker]["liq_sold_qty_dict"] = algorithm.liq_sold_qty_dict[ticker]
            sim_meta_data[ticker]["reg_exec"] = algorithm.reg_exec[ticker]

        sim_agent.append_run_data_to_db(timestamp, orig_account_snapshot_dict, action_msgs, sim_meta_data,
                                        stock_data_dict)
        # sim_agent.write_transaction_record(action_msgs)
        # sim_agent.write_acc_data()


# def main():
#     run_dir = "C:\\Users\\user\\Documents\\GitHub\\user_id_0\\backtest\\backtest_rebalance_margin_wif_max_drawdown_control_0\\"
#     graph_plotting_engine.plot_all_file_graph_png(f"{run_dir}\\run_data", "date", "NetLiquidation", f"{run_dir}/graph")

#
def main():
    run_file_dir = "C:\\Users\\lam\\Documents\\GitHub\\test_graph_data"
    sim_data_offline_engine = sim_data_io_engine.offline_engine(run_file_dir)
    backtest_data_directory = os.fsencode(run_file_dir)
    data_list = []
    for file in os.listdir(backtest_data_directory):
        if file.decode().endswith("csv"):
            file_name = file.decode().split(".csv")[0]
            stat_engine = statistic_engine(sim_data_offline_engine)
            print("stat_engine")
            sharpe_dict = stat_engine.get_sharpe_data(file_name)
            print("sharpe_dict")
            inception_sharpe = sharpe_dict.get("inception")
            _1_yr_sharpe = sharpe_dict.get("1y")
            _3_yr_sharpe = sharpe_dict.get("3y")
            _5_yr_sharpe = sharpe_dict.get("5y")
            _ytd_sharpe = sharpe_dict.get("ytd")

            return_dict = stat_engine.get_return_data(file_name)
            inception_return = return_dict.get("inception")
            _1_yr_return = return_dict.get("1y")
            _3_yr_return = return_dict.get("3y")
            _5_yr_return = return_dict.get("5y")
            _ytd_return = return_dict.get("ytd")

            all_file_stats_row = {
                "Backtest Spec": file_name, 'YTD Return': _ytd_return, '1 Yr Return': _1_yr_return,
                "3 Yr Return": _3_yr_return, "5 Yr Return": _5_yr_return,
                "Since Inception Return": inception_return, "Since Inception Sharpe": inception_sharpe,
                "YTD Sharpe": _ytd_sharpe,
                "1 Yr Sharpe": _1_yr_sharpe, "3 Yr Sharpe": _3_yr_sharpe, "5 Yr Sharpe": _5_yr_sharpe
            }
            # _additional_data = self.cal_additional_data(file_name)
            _additional_data = {}
            data_list.append(all_file_stats_row | _additional_data)

    col = ['Backtest Spec', 'YTD Return', '1 Yr Return', "3 Yr Return", "5 Yr Return",
           "Since Inception Return", "Since Inception Sharpe", "YTD Sharpe", "1 Yr Sharpe", "3 Yr Sharpe",
           "5 Yr Sharpe", "min(exliq/mkt value)"]
    df = pd.DataFrame(data_list, columns=col)
    df.fillna(0)
    df.to_csv(f"{run_file_dir}/test.csv")
    # print(f"{self.path}/stats_data/{self.table_name}.csv")
    # df.to_csv(f"{self.path}/stats_data/{self.table_name}.csv")
    pass


if __name__ == "__main__":
    main()
