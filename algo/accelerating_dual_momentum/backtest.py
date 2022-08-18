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
from engine.mongoDB_engine.write_statistic_document_engine import Write_Mongodb
from engine.stat_engine.statistic_engine import statistic_engine, realtime_statistic_engine
# from engine.mongoDB_engine.write_document_engine import Write_Mongodb
from object.backtest_acc_data import backtest_acc_data
from engine.visualisation_engine import graph_plotting_engine
from object.action_data import IBAction
import numpy as np


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
    store_mongoDB = False
    strategy_initial = 'None'
    video_link = 'None'
    documents_link = 'None'
    tags_array = list()
    subscribers_num = 0
    rating_dict = {}
    margin_ratio = np.NaN
    trader_name = "None"

    def __init__(self, tickers, bond, initial_amount, start_date, end_date, cal_stat, data_freq, user_id,
                 db_mode, store_mongoDB, strategy_initial='None', video_link='None', documents_link='None',
                 tags_array=list(), subscribers_num=0, rating_dict={}, margin_ratio=np.NaN, trader_name='None'):
        self.backtest_spec = None
        self.quick_test = False
        self.portfolio_data_engine = None
        self.run_file = None
        self.algorithm = None
        self.dividend_agent = None
        self.sim_agent = None
        self.trade_agent = None
        self.acc_data = None
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"

        self.table_info = {"mode": "backtest", "strategy_name": "accelerating_dual_momentum", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.tickers = tickers
        self.initial_amount = initial_amount
        self.start_date = start_date
        self.end_date = end_date
        self.start_timestamp = datetime.timestamp(start_date)
        self.end_timestamp = datetime.timestamp(end_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.db_mode = db_mode
        self.bond = bond
        self.indicators = {}

        delta_timestamps = self.cal_deltas_timestamps(start_date)
        for ticker in self.tickers:
            # get current ticker data
            self.stock_data_engines[ticker] = local_engine(ticker, self.data_freq)
            self.indicators[ticker] = Indicator(pd.DataFrame())
            self.get_indicator_ticker_items(ticker, delta_timestamps)
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
        if store_mongoDB:
            self.store_mongoDB = True
            self.strategy_initial = strategy_initial
            self.video_link = video_link
            self.documents_link = documents_link
            self.tags_array = tags_array
            self.subscribers_num = subscribers_num
            self.rating_dict = rating_dict
            self.margin_ratio = margin_ratio
            self.trader_name = trader_name

    def loop_through_param(self):
        print("start backtest")
        self.backtest_spec = {"large_etf": self.tickers[0], "small_etf": self.tickers[1], "bond": self.bond}
        spec_str = ""
        for k, v in self.backtest_spec.items():
            spec_str = f"{spec_str}{str(v)}_{str(k)}_"
        self.run_file = self.run_file_dir + spec_str + '.csv'
        graph_file = self.graph_dir + spec_str + '.png'
        if os.path.exists(self.run_file):
            df = pd.read_csv(self.run_file)
            first_day = df["date"].iloc[0]
            last_day = df["date"].iloc[-1]
            if abs((self.start_date - datetime.strptime(first_day, "%Y-%m-%d")).days) > 10 or \
                    abs((self.end_date - datetime.strptime(last_day, "%Y-%m-%d")).days) > 10:
                os.remove(Path(self.run_file))
            else:
                if os.path.exists(graph_file):
                    os.remove(Path(graph_file))
                self.load_run_data(spec_str)
        if os.path.exists(graph_file):
            os.remove(Path(graph_file))

        self.acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                          self.table_name, spec_str)
        options = self.tickers.copy()
        options.append(self.bond)

        self.portfolio_data_engine = backtest_portfolio_data_engine(self.acc_data, options)
        self.trade_agent = backtest_trade_engine(self.acc_data, self.stock_data_engines, self.portfolio_data_engine)
        self.sim_agent = simulation_agent(self.backtest_spec, self.table_info, False, self.portfolio_data_engine,
                                          self.tickers)
        self.dividend_agent = dividend_engine(self.tickers)
        self.algorithm = accelerating_dual_momentum(self.trade_agent, self.portfolio_data_engine)
        self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, self.algorithm,
                           self.portfolio_data_engine, self.sim_agent, self.dividend_agent, self.trade_agent)
        print("Finished Backtest:", self.backtest_spec)
        print("-------------------------------------------------------------------------------")

        self.plot_all_file_graph()
        list_of_stats_data = listdir(self.stats_data_dir)
        for file in list_of_stats_data:
            os.remove(Path(f"{self.stats_data_dir}/{file}"))

    def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm,
                      portfolio_data_engine, sim_agent, dividend_agent, trade_agent):
        if len(self.tickers) != 2:
            print('This strategy only works for two tickers')
            exit(0)
        print('start backtest')
        print('Fetch data')
        portfolio_data_engine.deposit_cash(initial_amount, start_timestamp)
        tickers_and_bonds_list = self.tickers.copy()
        tickers_and_bonds_list.append(self.bond)
        timestamps = \
            self.stock_data_engines[tickers_and_bonds_list[0]].get_data_by_range([start_timestamp, end_timestamp])[
                'timestamp']
        for x in range(1, len(tickers_and_bonds_list)):
            temp = self.stock_data_engines[tickers_and_bonds_list[x]].get_data_by_range(
                [start_timestamp, end_timestamp])['timestamp']
            timestamps = np.intersect1d(timestamps, temp)
        for timestamp in timestamps:
            _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, " " * 5, self.tickers, '#' * 20)
            if dividend_agent.check_div(timestamp):
                portfolio = portfolio_data_engine.get_portfolio()
                total_dividend = dividend_agent.distribute_div(timestamp, portfolio)
                if total_dividend != 0:
                    portfolio_data_engine.deposit_dividend(total_dividend, timestamp)

            if self.quick_test:
                if algorithm.check_exec(timestamp, freq="Monthly", relative_delta=1, ):
                    self.run(timestamp, algorithm, sim_agent, trade_agent)
            else:
                self.run(timestamp, algorithm, sim_agent, trade_agent)

    def run(self, timestamp, algorithm, sim_agent, trade_agent):
        pct_change_dict = {}
        for ticker in self.tickers:
            pct_change_dict.update({ticker: {}})
        sim_meta_data = {}
        stock_data_dict = {}
        for ticker in self.tickers:  # update ticker price
            ticker_engine = self.stock_data_engines[ticker]
            ticker_items = ticker_engine.get_ticker_item_by_timestamp(timestamp)
            self.indicators[ticker].append_into_df(ticker_items)
            pct_change_dict[ticker].update({1: self.indicators[ticker].get_pct_change(1, 'open', timestamp)})
            pct_change_dict[ticker].update({3: self.indicators[ticker].get_pct_change(3, 'open', timestamp)})
            pct_change_dict[ticker].update({6: self.indicators[ticker].get_pct_change(6, 'open', timestamp)})
            sim_meta_data.update({ticker: ticker_engine.get_ticker_item_by_timestamp(timestamp)})
            price = ticker_items.get('open')
            if price is None:
                stock_data_dict.update({ticker: {'last': None}})
                continue
            else:
                stock_data_dict.update({ticker: {'last': price}})
        bond_engine = self.stock_data_engines[self.bond]  # update bond price
        ticker_items = bond_engine.get_ticker_item_by_timestamp(timestamp)
        sim_meta_data.update({self.bond: bond_engine.get_ticker_item_by_timestamp(timestamp)})
        price = ticker_items.get('open')
        if price is None:
            stock_data_dict.update({self.bond: {'last': None}})
        else:
            stock_data_dict.update({self.bond: {'last': price}})

        action_msgs = algorithm.run(pct_change_dict, stock_data_dict, self.bond, timestamp)
        action_record = []
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

    def cal_deltas_timestamps(self, start_date):

        delta_timestamps = []
        for month in range(1, 7):
            delta = timedelta(weeks=(4 * month))
            date_before = start_date - delta
            date_before_timestamp = datetime.timestamp(date_before)
            delta_timestamps.append(date_before_timestamp)
        return delta_timestamps

    def get_indicator_ticker_items(self, ticker, delta_timestamps):
        # find the ticker data  1 ,3 ,6 month before
        for month in range(5, -1, -1):
            timestamp = delta_timestamps[month]
            ticker_item = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(timestamp)
            while ticker_item is None:
                timestamp = timestamp + 3600
                ticker_item = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(timestamp)
            self.indicators[ticker].append_into_df(ticker_item)

    def plot_all_file_graph(self):
        print("plot_graph")
        graph_plotting_engine.plot_all_file_graph_png(f"{self.run_file_dir}", "date", "NetLiquidation",
                                                      f"{self.path}/{self.table_name}/graph")
    def load_run_data(self, spec_str):
        run_file = self.run_file_dir + spec_str + '.csv'
        self.acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                          self.table_name, spec_str)
        self.portfolio_data_engine = backtest_portfolio_data_engine(self.acc_data, self.tickers)
        self.trade_agent = backtest_trade_engine(self.acc_data, self.stock_data_engines,
                                                 self.portfolio_data_engine)
        self.sim_agent = simulation_agent(self.backtest_spec, self.table_info, False, self.portfolio_data_engine,
                                          self.tickers)
        self.dividend_agent = dividend_engine(self.tickers)
        self.stat_agent = realtime_statistic_engine(self.run_file_dir, self.start_timestamp, self.end_timestamp,
                                                    self.path, self.table_name, self.store_mongoDB,
                                                    self.stats_data_dir, self.strategy_initial, self.video_link,
                                                    self.documents_link, self.tags_array, self.rating_dict,
                                                    self.margin_ratio, self.subscribers_num, self.trader_name)
        self.algorithm = accelerating_dual_momentum(self.trade_agent, self.portfolio_data_engine)
        df = pd.read_csv(run_file)
        row = df.iloc[-1]
        last_day = df["date"].iloc[-1]
        availablefunds = row.get("AvailableFunds")
        excessliquidity = row.get("ExcessLiquidity")
        buyingpower = row.get("BuyingPower")
        leverage = row.get("Leverage")
        equitywithloanvalue = row.get("EquityWithLoanValue")
        totalcashvalue = row.get("TotalCashValue")
        netdividend = row.get("NetDividend")
        netliquidation = row.get("NetLiquidation")
        unrealizedpnL = row.get("UnrealizedPnL")
        realizedpnL = row.get("RealizedPnL")
        grosspositionvalue = row.get("GrossPositionValue")

        self.acc_data.update_trading_funds(availablefunds, excessliquidity, buyingpower, leverage, equitywithloanvalue)
        self.acc_data.update_mkt_value(totalcashvalue, netdividend, netliquidation, unrealizedpnL, realizedpnL,
                                       grosspositionvalue)
        for ticker in self.tickers:
            mktprice = row.get(f"marketPrice_{ticker}")
            position = row.get(f"position_{ticker}")
            averagecost = row.get(f"averageCost_{ticker}")
            marketvalue = row.get(f"marketValue_{ticker}")
            ticker_realizedpnl = row.get(f"realizedPNL_{ticker}")
            ticker_unrealizedpnl = row.get(f"unrealizedPNL_{ticker}")
            initmarginreq = row.get(f"initMarginReq_{ticker}")
            maintmarginreq = row.get(f"maintMarginReq_{ticker}")
            costbasis = row.get(f"costBasis_{ticker}")
            self.acc_data.update_portfolio_item(ticker, position, mktprice, averagecost, marketvalue,
                                                ticker_realizedpnl, ticker_unrealizedpnl, initmarginreq, maintmarginreq,
                                                costbasis)
        self.portfolio_data_engine.acc_data = self.acc_data
        self.trade_agent.backtest_acc_data = self.acc_data
