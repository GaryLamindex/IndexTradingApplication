import os
import pathlib
from datetime import datetime

import numpy
import pandas as pd
from os import listdir
from pathlib import Path
from algo.portfolio_rebalance.algorithm import portfolio_rebalance
from engine.backtest_engine.dividend_engine import dividend_engine
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from application.realtime_statistic_application import realtime_statistic_engine
from object.backtest_acc_data import backtest_acc_data
from engine.visualisation_engine import graph_plotting_engine
from object.action_data import IBAction
import numpy as np


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
    list_of_tickers = []
    initial_amount = 0
    stock_data_engines = {}
    timestamps = []
    rebalance_ratio = []
    quick_test = True
    tickers = []
    store_mongoDB = False
    strategy_initial = 'None'
    video_link = 'None'
    documents_link = 'None'
    tags_array = list()
    subscribers_num = 0
    rating_dict = {}
    margin_ratio = np.NaN
    trader_name = "None"
    end_date = None

    def __init__(self, list_of_tickers, initial_amount, start_date, end_date, cal_stat, data_freq, user_id,
                 db_mode, quick_test, acceptance_range, list_of_rebalance_ratios, store_mongoDB,
                 strategy_initial='None',
                 video_link='None', documents_link='None', tags_array=list(), subscribers_num=0,
                 rating_dict={}, margin_ratio=np.NaN, trader_name='None'):

        self.stat_agent = None
        self.algorithm = None
        self.dividend_agent = None
        self.sim_agent = None
        self.trade_agent = None
        self.portfolio_data_engine = None
        self.acc_data = None
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"

        self.table_info = {"mode": "backtest", "strategy_name": "portfolio_rebalance", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.list_of_tickers = list_of_tickers
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.end_timestamp = datetime.timestamp(end_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.quick_test = quick_test
        self.db_mode = db_mode
        self.acceptance_range = acceptance_range
        self.rebalance_ratio = list_of_rebalance_ratios
        self.start_date = start_date
        self.end_date = end_date
        self.tickers = []

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
        # loop through all the rebalance requirement
        for x in range(len(self.list_of_tickers)):
            ratio = self.rebalance_ratio[x].copy()
            self.tickers = self.list_of_tickers[x].copy()
            for ticker in self.tickers:
                self.stock_data_engines[ticker] = local_engine(ticker, self.data_freq)
            num_tickers = len(self.tickers)
            print("Start Backtest:", ratio)
            self.rebalance_dict = {}
            for ticker_num in range(num_tickers):
                self.rebalance_dict.update({self.tickers[ticker_num]: ratio[ticker_num]})

            if self.check_rebalance_ratio():
                backtest_spec = self.rebalance_dict
                spec_str = ""
                for k, v in backtest_spec.items():
                    spec_str = f"{spec_str}{str(v)}_{str(k)}_"

                # remove if exist
                run_file = self.run_file_dir + spec_str + '.csv'
                graph_file = self.graph_dir + spec_str + '.png'
                if os.path.exists(run_file):
                    df = pd.read_csv(run_file)
                    first_day = df["date"].iloc[0]
                    last_day = df["date"].iloc[-1]
                    first_row=df.iloc[0]
                    last_row = df.iloc[-1]
                    if abs((self.start_date.replace(day=1) - datetime.strptime(first_day, "%Y-%m-%d")).days) > 10 or \
                            abs((self.end_date.replace(day=1) - datetime.strptime(last_day, "%Y-%m-%d")).days) > 10:
                        os.remove(Path(run_file))
                    else:
                        if os.path.exists(graph_file):
                            os.remove(Path(graph_file))
                        self.load_run_data(spec_str)
                        continue
                if os.path.exists(graph_file):
                    os.remove(Path(graph_file))

                self.acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                                  self.table_name, spec_str)
                self.portfolio_data_engine = backtest_portfolio_data_engine(self.acc_data, self.tickers)
                self.trade_agent = backtest_trade_engine(self.acc_data, self.stock_data_engines,
                                                         self.portfolio_data_engine)
                self.sim_agent = simulation_agent(self.rebalance_dict, self.table_info, False,
                                                  self.portfolio_data_engine,
                                                  self.tickers)
                self.dividend_agent = dividend_engine(self.tickers)
                self.stat_agent = realtime_statistic_engine(self.run_file_dir, self.start_timestamp, self.end_timestamp,
                                                            self.path, self.table_name, self.store_mongoDB,
                                                            self.stats_data_dir, self.strategy_initial, self.video_link,
                                                            self.documents_link, self.tags_array, self.rating_dict,
                                                            self.margin_ratio, self.subscribers_num, self.trader_name)

                self.algorithm = portfolio_rebalance(self.trade_agent, self.portfolio_data_engine, self.rebalance_dict,
                                                     self.acceptance_range)
                self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, self.algorithm,
                                   self.portfolio_data_engine, self.sim_agent, self.dividend_agent, self.trade_agent)
                print("Finished Backtest:", backtest_spec)
                print("-------------------------------------------------------------------------------")
        self.plot_all_file_graph()
        list_of_stats_data = listdir(self.stats_data_dir)
        for file in list_of_stats_data:
            os.remove(Path(f"{self.stats_data_dir}/{file}"))
        if self.cal_stat:
            self.stat_agent.cal_all_file_return()

    def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm, portfolio_data_engine,
                      sim_agent, dividend_engine, trade_agent):
        # connect to downloaded ib data to get price data
        row = 0
        timestamps = self.stock_data_engines[self.tickers[0]].get_data_by_range([start_timestamp, end_timestamp])[
            'timestamp']
        for x in range(1, len(self.tickers)):
            temp = self.stock_data_engines[self.tickers[x]].get_data_by_range(
                [start_timestamp, end_timestamp])['timestamp']
            timestamps = np.intersect1d(timestamps, temp)
        for timestamp in timestamps:
            _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, '#' * 20)

            if row == 0:
                # input initial cash
                portfolio_data_engine.deposit_cash(initial_amount, timestamp)
                row += 1

            if dividend_engine.check_div(timestamp):
                portfolio = portfolio_data_engine.get_portfolio()
                total_dividend = dividend_engine.distribute_div(timestamp, portfolio)
                if total_dividend != 0:
                    portfolio_data_engine.deposit_dividend(total_dividend, timestamp)

            if self.quick_test:
                if algorithm.check_exec(timestamp, freq="Monthly", relative_delta=1,):
                    self.run(timestamp, algorithm, sim_agent, trade_agent)
            else:
                self.run(timestamp, algorithm, sim_agent, trade_agent)

    def check_rebalance_ratio(self):
        check_ratio = False
        total_ratio = 0
        for k, v in self.rebalance_dict.items():
            ratio = v / 100
            total_ratio += ratio
        if abs((1 - total_ratio)) > 0.01:
            print("total ratio is not 100%")
        else:
            check_ratio = True
        return check_ratio

    def plot_all_file_graph(self):
        print("plot_graph")
        graph_plotting_engine.plot_all_file_graph_png(f"{self.run_file_dir}", "date", "NetLiquidation",
                                                      f"{self.path}/{self.table_name}/graph")

    #
    # def cal_all_file_month_to_month_return(self):
    #     stats_agent = statistic_agent(self.db_path)
    #     list_of_files = listdir(self.sim_data_path + "/csv")
    #
    #     for file in list_of_files:
    #         _data = stats_agent.cal_month_to_month_breakdown(file)
    #     pass

    def run(self, timestamp, algorithm, sim_agent, trade_agent):
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

        # input database and historical data into algo and get action msgs
        action_msgs = algorithm.run(stock_data_dict, timestamp)

        # execute action msgs
        action_record = []
        if action_msgs is None:
            sim_agent.append_run_data_to_db(timestamp, orig_account_snapshot_dict, action_record, sim_meta_data,
                                            stock_data_dict)
        else:
            for action_msg in action_msgs:
                action = action_msg.action_enum
                if action == IBAction.SELL_MKT_ORDER:
                    temp_action_record = trade_agent.place_sell_stock_mkt_order(action_msg.args_dict.get("ticker"),
                                                                                action_msg.args_dict.get(
                                                                                    "position_sell"),
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
            sim_agent.append_run_data_to_db(timestamp, orig_account_snapshot_dict, action_record, sim_meta_data,
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

    def load_run_data(self, spec_str):
        run_file = self.run_file_dir + spec_str + '.csv'
        self.acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                          self.table_name, spec_str)
        self.portfolio_data_engine = backtest_portfolio_data_engine(self.acc_data, self.tickers)
        self.trade_agent = backtest_trade_engine(self.acc_data, self.stock_data_engines,
                                                 self.portfolio_data_engine)
        self.sim_agent = simulation_agent(self.rebalance_dict, self.table_info, False,
                                          self.portfolio_data_engine,
                                          self.tickers)
        self.dividend_agent = dividend_engine(self.tickers)
        self.stat_agent = realtime_statistic_engine(self.run_file_dir, self.start_timestamp, self.end_timestamp,
                                                    self.path, self.table_name, self.store_mongoDB,
                                                    self.stats_data_dir, self.strategy_initial, self.video_link,
                                                    self.documents_link, self.tags_array, self.rating_dict,
                                                    self.margin_ratio, self.subscribers_num, self.trader_name)
        self.algorithm = portfolio_rebalance(self.trade_agent, self.portfolio_data_engine, self.rebalance_dict,
                                             self.acceptance_range)
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

        pass


def main():
    pass


if __name__ == "__main__":
    main()
