import os
import pathlib
from datetime import datetime
import pandas as pd
from os import listdir
from pathlib import Path
from algo.portfolio_rebalance.algorithm import portfolio_rebalance
from engine.backtest_engine.dividend_engine import dividend_engine
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.simulation_engine import sim_data_io_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from engine.simulation_engine.statistic_engine import statistic_engine
from engine.mongoDB_engine.write_document_engine import Write_Mongodb
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

            # list_of_run_files = listdir(self.run_file_dir)
            # list_of_stats_data = listdir(self.stats_data_dir)
            # list_of_acc_data = listdir(self.acc_data_dir)
            # list_of_transact_data = listdir(self.transact_data_dir)
            # list_of_graph = listdir(self.graph_dir)

            # for file in list_of_run_files:
            #     os.remove(Path(f"{self.run_file_dir}/{file}"))
            # for file in list_of_stats_data:
            #     os.remove(Path(f"{self.stats_data_dir}/{file}"))
            # for file in list_of_acc_data:
            #     os.remove(Path(f"{self.acc_data_dir}/{file}"))
            # for file in list_of_transact_data:
            #     os.remove(Path(f"{self.transact_data_dir}/{file}"))
            # for file in list_of_graph:
            #             #     os.remove(Path(f"{self.graph_dir}/{file}"))

    def loop_through_param(self):
        # loop through all the rebalance requirement
        # calculate all possible ratio that sum is 100 with different number of stickers
        for ratio in self.rebalance_ratio:
            num_tickers = len(self.tickers)
            print("Start Backtest:", ratio)
            self.rebalance_dict = {}
            for ticker_num in range(num_tickers):
                self.rebalance_dict.update({self.tickers[ticker_num]: ratio[ticker_num]})
            self.check_rebalance_ratio()
            if self.check_ratio:
                backtest_spec = self.rebalance_dict
                spec_str = ""
                for k, v in backtest_spec.items():
                    spec_str = f"{spec_str}{str(v)}_{str(k)}_"

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
                sim_agent = simulation_agent(self.rebalance_dict, self.table_info, False, portfolio_data_engine,
                                             self.tickers)
                dividend_agent = dividend_engine(self.tickers)

                algorithm = portfolio_rebalance(trade_agent, portfolio_data_engine, self.rebalance_dict,
                                                self.acceptance_range)
                self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, algorithm,
                                   portfolio_data_engine, sim_agent, dividend_agent)
                print("Finished Backtest:", backtest_spec)
                print("-------------------------------------------------------------------------------")
        self.plot_all_file_graph()
        list_of_stats_data = listdir(self.stats_data_dir)
        for file in list_of_stats_data:
            os.remove(Path(f"{self.stats_data_dir}/{file}"))
        if self.cal_stat:
            self.cal_all_file_return()

    def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm, portfolio_data_engine,
                      sim_agent, dividend_engine):
        # connect to downloaded ib data to get price data
        row = 0
        timestamps = {}
        timestamps = self.stock_data_engines[self.tickers[0]].get_data_by_range([start_timestamp, end_timestamp])[
            'timestamp']
        for timestamp in timestamps:
            _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, '#' * 20)

            if row == 0:
                # input initial cash
                portfolio_data_engine.deposit_cash(initial_amount, timestamp)
                row += 1
            # dividend_engine.check_div(timestamp)
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
        sim_data_offline_engine = sim_data_io_engine.offline_engine(self.run_file_dir)
        backtest_data_directory = os.fsencode(self.run_file_dir)
        data_list = []
        for idx, file in enumerate(os.listdir(backtest_data_directory)):
            if file.decode().endswith("csv"):
                file_name = file.decode().split(".csv")[0]
                stat_engine = statistic_engine(sim_data_offline_engine)
                # stat_engine_3 = statistic_engine_3(sim_data_offline_engine)
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

                marketCol = f"marketPrice_{self.tickers[0]}"
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

                win_rate_dict = stat_engine.get_win_rate_data(file_name)
                inception_win_rate = win_rate_dict.get('inception')
                _1_yr_win_rate = win_rate_dict.get('1y')
                _3_yr_win_rate = win_rate_dict.get('3y')
                _5_yr_win_rate = win_rate_dict.get('5y')
                _ytd_win_rate = win_rate_dict.get('ytd')

                dateStringS = datetime.fromtimestamp(self.start_timestamp)
                dateStringE = datetime.fromtimestamp(self.end_timestamp)
                date_range = [f"{dateStringS.year}-{dateStringS.month}-{dateStringS.day}",
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

                drawdown_abstract, drawdown_raw_data = stat_engine.get_drawdown_data(file_name, date_range)
                # drawdown_abstract = drawdown_dict.get('drawdown_abstract')
                # drawdown_raw_data = drawdown_dict.get('drawdown_raw_data')

                average_win_day_dict = stat_engine.get_average_win_day_data(file_name)
                inception_average_win_day = average_win_day_dict.get('inception')
                _1_yr_average_win_day = average_win_day_dict.get('1y')
                _3_yr_average_win_day = average_win_day_dict.get('3y')
                _5_yr_average_win_day = average_win_day_dict.get('5y')
                _ytd_average_win_day = average_win_day_dict.get('ytd')

                profit_loss_ratio_dict = stat_engine.get_profit_loss_ratio_data(file_name)
                inception_profit_loss_ratio = profit_loss_ratio_dict.get('inception')
                _1_yr_profit_loss_ratio = profit_loss_ratio_dict.get('1y')
                _3_yr_profit_loss_ratio = profit_loss_ratio_dict.get('3y')
                _5_yr_profit_loss_ratio = profit_loss_ratio_dict.get('5y')
                _ytd_profit_loss_ratio = profit_loss_ratio_dict.get('ytd')

                composite_dict = stat_engine.get_composite_data(file_name)

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
                    # "Drawdown_abstract": drawdown_abstract, "Drawdown_raw_data": drawdown_raw_data,

                    "Since Inception Average Win Per Day": inception_average_win_day,
                    "YTD Average Win Per Day": _ytd_average_win_day, "1 Yr Average Win Per Day": _1_yr_average_win_day,
                    "3 Yr Average Win Per Day": _3_yr_average_win_day,
                    "5 Yr Average Win Per Day": _5_yr_average_win_day,
                    "Since Inception Profit Loss Ratio": inception_profit_loss_ratio,
                    "YTD Profit Loss Ratio": _ytd_profit_loss_ratio, "1 Yr Profit Loss Ratio": _1_yr_profit_loss_ratio,
                    "3 Yr Profit Loss Ratio": _3_yr_profit_loss_ratio,
                    "5 Yr Profit Loss Ratio": _5_yr_profit_loss_ratio,

                    "Composite": composite_dict

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
               # "Drawdown_abstract", "Drawdown_raw_data",
               "Since Inception Average Win Per Day", "YTD Average Win Per Day", "1 Yr Average Win Per Day",
               "3 Yr Average Win Per Day", "5 Yr Average Win Per Day",
               "Since Inception Profit Loss Ratio", "YTD Profit Loss Ratio", "1 Yr Profit Loss Ratio",
               "3 Yr Profit Loss Ratio", "5 Yr Profit Loss Ratio",
               "Composite"
               ]

        df = pd.DataFrame(data_list, columns=col)
        df.fillna(0)
        print(f"{self.path}/stats_data/{self.table_name}.csv")
        df.to_csv(f"{self.path}/{self.table_name}/stats_data/all_file_return.csv", index=False)

        drawdown_raw_data.to_csv(f"{self.path}/{self.table_name}/stats_data/drawdown_raw_data.csv", index=False)
        drawdown_abstract.to_csv(f"{self.path}/{self.table_name}/stats_data/drawdown_abstract.csv", index=False)

        # _p = df.to_dict(orient='records')
        # wmdb = Write_Mongodb()
        # wmdb.write_one_min_raw_data('Strategies', _p)
        pass

    #
    # def cal_all_file_month_to_month_return(self):
    #     stats_agent = statistic_agent(self.db_path)
    #     list_of_files = listdir(self.sim_data_path + "/csv")
    #
    #     for file in list_of_files:
    #         _data = stats_agent.cal_month_to_month_breakdown(file)
    #     pass

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
    def get_outcomes(dim, target):

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
