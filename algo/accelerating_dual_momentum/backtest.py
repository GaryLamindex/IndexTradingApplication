import os
import pathlib
from datetime import datetime
from datetime import timedelta
import pandas as pd
from os import listdir
from pathlib import Path
from algo.accelerating_dual_momentum.algorithm import accelerating_dual_momentum
from algo.accelerating_dual_momentum.indicator import Indicator
from engine.backtest_engine.dividend_engine import dividend_engine
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.simulation_engine import sim_data_io_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from engine.mongoDB_engine.write_document_engine import Write_Mongodb
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
    indicators = {}

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
        self.indicators = {}

        one_month_delta = timedelta(weeks=4)
        three_month_delta = timedelta(weeks=12)
        six_month_delta = timedelta(weeks=24)
        # calculate the date of 1, 3, 6 month before
        one_month_before = start_date - one_month_delta
        one_month_before_timestamp = datetime.timestamp(one_month_before)
        three_month_before = start_date - three_month_delta
        three_month_before_timestamp = datetime.timestamp(three_month_before)
        six_month_before = start_date - six_month_delta
        six_month_before_timestamp = datetime.timestamp(six_month_before)

        for ticker in self.tickers:
            # get current ticker data
            self.stock_data_engines[ticker] = local_engine(ticker, self.data_freq)
            self.indicators[ticker] = Indicator(pd.DataFrame())
            # find the ticker data  1 ,3 ,6 month before
            six_month_ticker_items = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(
                six_month_before_timestamp)
            while six_month_ticker_items is None:
                six_month_ticker_items = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(
                    six_month_before_timestamp + 1)
            three_month_ticker_items = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(
                three_month_before_timestamp)
            while three_month_ticker_items is None:
                three_month_ticker_items = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(
                    three_month_before_timestamp + 1)
            one_month_ticker_items = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(
                one_month_before_timestamp)
            while one_month_ticker_items is None:
                one_month_ticker_items = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(
                    one_month_before_timestamp + 1)
            self.indicators[ticker].append_into_df(six_month_ticker_items)
            self.indicators[ticker].append_into_df(three_month_ticker_items)
            self.indicators[ticker].append_into_df(one_month_ticker_items)
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
        print("start backtest")
        backtest_spec = {"large_etf": self.tickers[0], "small_etf": self.tickers[1], "bond": self.bond}
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
        options = self.tickers.copy()
        options.append(self.bond)

        portfolio_data_engine = backtest_portfolio_data_engine(acc_data, options)
        trade_agent = backtest_trade_engine(acc_data, self.stock_data_engines, portfolio_data_engine)
        sim_agent = simulation_agent(backtest_spec, self.table_info, False, portfolio_data_engine,
                                     self.tickers)
        dividend_agent = dividend_engine(self.tickers)
        algorithm = accelerating_dual_momentum(trade_agent, portfolio_data_engine)
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
        timestamps = self.stock_data_engines[self.tickers[0]].get_intersect_timestamps(series_1, series_2)
        for timestamp in timestamps:
            _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, '#' * 20)
            if algorithm.check_exec(timestamp, freq="Monthly", relative_delta=1):
                self.run(timestamp, algorithm, sim_agent, trade_agent, portfolio_data_engine)

    def run(self, timestamp, algorithm, sim_agent, trade_agent, portfolio_data_engine):
        pct_change_dict = {}
        for ticker in self.tickers:
            pct_change_dict.update({ticker: {}})
        if timestamp == 1199284200:
            a=0
        sim_meta_data = {}
        stock_data_dict = {}
        for ticker in self.tickers:  # update ticker price
            ticker_engine = self.stock_data_engines[ticker]
            ticker_items = ticker_engine.get_ticker_item_by_timestamp(timestamp)
            self.indicators[ticker].append_into_df(ticker_items)
            pct_change_dict[ticker].update({1: self.indicators[ticker].get_pct_change(1, 'open', timestamp)})
            pct_change_dict[ticker].update({3: self.indicators[ticker].get_pct_change(2, 'open', timestamp)})
            pct_change_dict[ticker].update({6: self.indicators[ticker].get_pct_change(3, 'open', timestamp)})
            sim_meta_data.update({ticker: ticker_engine.get_ticker_item_by_timestamp(timestamp)})
            price = ticker_items.get('open')
            if price is None:
                stock_data_dict.update({ticker: {'last': None}})
                continue
            else:
                stock_data_dict.update({ticker: {'last': price}})
        ticker_engine = self.stock_data_engines[self.bond]  # update bond price
        ticker_items = ticker_engine.get_ticker_item_by_timestamp(timestamp)
        sim_meta_data.update({self.bond: ticker_engine.get_ticker_item_by_timestamp(timestamp)})
        price = ticker_items.get('open')
        if price is None:
            stock_data_dict.update({self.bond: {'last': None}})
        else:
            stock_data_dict.update({self.bond: {'last': price}})

        action_msgs = algorithm.run(stock_data_dict, self.bond)
        action_record = []
        if action_msgs is None:
            a=0
        for action_msg in action_msgs:
            action = action_msg.action_enum
            if action == IBAction.SELL_MKT_ORDER:
                temp_action_record = trade_agent.place_sell_stock_mkt_order(action_msg.args_dict.get("ticker"),
                                                                            action_msg.args_dict.get("position_sell"),
                                                                            {"timestamp": action_msg.timestamp})
                action_record.append(temp_action_record)
        for action_msg in action_msgs:
            action = action_msg.action_enum
            if action == IBAction.BUY_MKT_ORDER:
                temp_action_record = trade_agent.place_buy_stock_mkt_order(action_msg.args_dict.get("ticker"),
                                                                           action_msg.args_dict.get(
                                                                               "position_purchase"),
                                                                           {"timestamp": action_msg.timestamp})
                action_record.append(temp_action_record)
        sim_agent.append_run_data_to_db(timestamp, sim_agent.portfolio_data_engine.get_account_snapshot(),
                                        action_record, sim_meta_data,
                                        stock_data_dict)

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
                marketCol = f'marketPrice_{self.tickers[0]}'
                costCol = f'costBasis_{self.tickers[idx]}'
                valueCol = f'marketValue_{self.tickers[idx]}'
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

                return_dict, return_inflation_adj_dict, compound_return_dict = stat_engine.get_return_data(file_name)
                inception_return = return_dict.get("inception")
                _1_yr_return = return_dict.get("1y")
                _3_yr_return = return_dict.get("3y")
                _5_yr_return = return_dict.get("5y")
                _ytd_return = return_dict.get("ytd")
                inflation_adj_inception_return = return_inflation_adj_dict.get('inception')
                inflation_adj_1_yr_return = return_inflation_adj_dict.get('1y')
                inflation_adj_3_yr_return = return_inflation_adj_dict.get('3y')
                inflation_adj_5_yr_return = return_inflation_adj_dict.get('5y')
                inflation_adj_ytd_return = return_inflation_adj_dict.get('ytd')
                compound_inception_return_dict = compound_return_dict.get('inception')
                compound_1_yr_return_dict = compound_return_dict.get('1y')
                compound_3_yr_return_dict = compound_return_dict.get('3y')
                compound_5_yr_return_dict = compound_return_dict.get('5y')
                compound_ytd_return_dict = compound_return_dict.get('ytd')

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

                win_rate_dict = stat_engine.get_win_rate_data(file_name)
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

                ########## Store drawdown in another csv
                drawdown_abstract, drawdown_raw_data = stat_engine.get_drawdown_data(file_name, date_range)
                # drawdown_dict = stat_engine.get_drawdown_data(file_name, date_range)
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

                last_nlv = stat_engine.get_last_nlv(file_name)
                last_daily = stat_engine.get_last_daily_change(file_name)
                last_monthly = stat_engine.get_last_daily_change(file_name)

                composite_dict, number_of_ETFs = stat_engine.get_composite_data(file_name)

                sd_dict = stat_engine.get_sd_data(file_name)
                _1_yr_sd = sd_dict.get('1y')
                _3_yr_sd = sd_dict.get('3y')
                _5_yr_sd = sd_dict.get('5y')
                inception_sd = sd_dict.get('inception')

                pos_neg_dict = stat_engine.get_pos_neg_data(file_name)
                _1_yr_pos_neg = pos_neg_dict.get('1y')
                _3_yr_pos_neg = pos_neg_dict.get('3y')
                _5_yr_pos_neg = pos_neg_dict.get('5y')
                inception_pos_neg = pos_neg_dict.get('inception')

                net_profit = stat_engine.get_net_profit_inception(file_name)

                all_file_stats_row = {
                    "Backtest Spec": file_name, 'YTD Return': _ytd_return, '1 Yr Return': _1_yr_return,
                    "3 Yr Return": _3_yr_return, "5 Yr Return": _5_yr_return,
                    "Since Inception Return": inception_return,
                    'inflation adj YTD Return': inflation_adj_ytd_return,
                    'inflation adj 1 Yr Return': inflation_adj_1_yr_return,
                    'inflation adj 3 Yr Return': inflation_adj_3_yr_return,
                    'inflation adj 5 yr Return': inflation_adj_5_yr_return,
                    'inflation adj Inception Return': inflation_adj_inception_return,
                    "Since Inception Sharpe": inception_sharpe,
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
                    "last nlv": last_nlv, "last daily change": last_daily, "last monthly change": last_monthly,

                    "Composite": composite_dict,
                    "number_of_ETFs": number_of_ETFs,

                    "1 yr sd": _1_yr_sd,
                    "3 yr sd": _3_yr_sd,
                    "5 yr sd": _5_yr_sd,
                    "inception sd": inception_sd,

                    "1 yr pos neg": _1_yr_pos_neg,
                    "3 yr pos neg": _3_yr_pos_neg,
                    "5 yr pos neg": _5_yr_pos_neg,
                    "inception pos neg": inception_pos_neg,
                    "net profit": net_profit,
                    "compound_inception_return_dict": compound_inception_return_dict,
                    "compound_1_yr_return_dict": compound_1_yr_return_dict,
                    "compound_3_yr_return_dict": compound_3_yr_return_dict,
                    "compound_5_yr_return_dict": compound_5_yr_return_dict,
                    "compound_ytd_return_dict": compound_ytd_return_dict
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
               # "Drawdown_abstract","Drawdown_raw_data",
               "Since Inception Average Win Per Day", "YTD Average Win Per Day", "1 Yr Average Win Per Day",
               "3 Yr Average Win Per Day", "5 Yr Average Win Per Day",
               "Since Inception Profit Loss Ratio", "YTD Profit Loss Ratio", "1 Yr Profit Loss Ratio",
               "3 Yr Profit Loss Ratio", "5 Yr Profit Loss Ratio",
               "last nlv", "last daily change", "last monthly change",
               "Composite", "number_of_ETFs",
               "1 yr sd", "3 yr sd", "5 yr sd", "inception sd", "_1_yr_pos_neg", "_3_yr_pos_neg", "_5_yr_pos_neg",
               "inception_pos_neg", "net profit",
               "compound_inception_return_dict", "compound_1_yr_return_dict", "compound_3_yr_return_dict",
               "compound_5_yr_return_dict", "compound_ytd_return_dict"
               ]

        df = pd.DataFrame(data=data_list, columns=col)
        # pd.set_option("max_colwidth", 10000)
        df.fillna(0)
        print(f"{self.path}/stats_data/{self.table_name}.csv")
        df.to_csv(f"{self.path}/{self.table_name}/stats_data/all_file_return.csv", index=False)

        drawdown_raw_data.to_csv(f"{self.path}/{self.table_name}/stats_data/drawdown_raw_data.csv", index=False)
        drawdown_abstract.to_csv(f"{self.path}/{self.table_name}/stats_data/drawdown_abstract.csv", index=False)

        # store data to mongoDB HERE
        if self.store_mongoDB:
            print("(*&^%$#$%^&*()(*&^%$#$%^&*(")
            p = Write_Mongodb()
            for file in os.listdir(backtest_data_directory):
                if file.decode().endswith("csv"):
                    csv_path = Path(self.run_file_dir, file.decode())
                    a = pd.read_csv(csv_path)
                    p.write_new_backtest_result(strategy_name=self.table_name,
                                                drawdown_abstract_df=drawdown_abstract,
                                                drawdown_raw_df=drawdown_raw_data,
                                                run_df=a,
                                                all_file_return_df=df,
                                                strategy_initial=self.strategy_initial,
                                                video_link=self.video_link,
                                                documents_link=self.documents_link,
                                                tags_array=self.tags_array,
                                                rating_dict=self.rating_dict,
                                                margin_ratio=self.margin_ratio,
                                                subscribers_num=self.subscribers_num,
                                                trader_name=self.trader_name)
