import time

import numpy as np
import pandas as pd
from datetime import datetime
from algo.accelerating_dual_momentum.backtest import backtest as accelerating_dual_momentum_backtest
from object.action_data import IBAction, IBActionsTuple
from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.visualisation_engine import graph_plotting_engine
import os
import pathlib
from pathlib import Path
from engine.mongoDB_engine.write_run_data_document_engine import Write_Mongodb


class realtime:
    def __init__(self, tickers, bond, initial_amount, start_date, cal_stat, data_freq, user_id,
                 db_mode,  execute_period):
        self.stat_agent = None
        self.rebalance_dict = None
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
        self.bond = bond
        self.indicators = {}
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/realtime"

        self.table_info = {"mode": "realtime", "strategy_name": "portfolio_rebalance", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.user_id = user_id
        self.tickers = tickers
        self.bond = bond
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.db_mode = db_mode
        self.start_date = start_date
        self.backtest = None
        self.now = datetime.now()
        self.execute_period = execute_period
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

    def init_backtest(self, user_id,  store_mongoDB, strategy_initial, video_link,
                      documents_link, tags_array, subscribers_num,
                      rating_dict, margin_ratio, trader_name):
        self.now = datetime.now()
        # self.now = datetime(2022, 2, 3)
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

        self.backtest = accelerating_dual_momentum_backtest(self.tickers, self.bond, self.initial_amount,
                                                            self.start_date, self.now, True, self.data_freq,
                                                            user_id, self.db_mode, store_mongoDB,
                                                            strategy_initial,
                                                            video_link, documents_link, tags_array, self.subscribers_num,
                                                            self.rating_dict, self.margin_ratio, self.trader_name
                                                            )
        self.backtest.loop_through_param()

    def run(self):
        if not self.init_backtest_flag:
            self.init_backtest(self.user_id,
                               store_mongoDB=False,
                               strategy_initial='SPY_MSFT_TIP_accelerating_dual_momentum',
                               video_link='https://www.youtube.com',
                               documents_link='https://google.com',
                               tags_array=None,
                               subscribers_num=3,
                               rating_dict=None,
                               margin_ratio=3.24,
                               trader_name='Fai'
                               )
            self.init_backtest_flag = True
        else:
            self.acc_data = self.backtest.acc_data
            self.portfolio_data_engine = self.backtest.portfolio_data_engine
            self.trade_agent = self.backtest.trade_agent
            self.sim_agent = self.backtest.sim_agent
            self.dividend_agent = self.backtest.dividend_agent
            self.algorithm = self.backtest.algorithm
            for ticker in self.tickers:
                self.stock_data_engines[ticker] = local_engine(ticker, self.data_freq)
            df = pd.read_csv(self.backtest.run_file)
            last_excute_timestamp = df["timestamp"].iloc[-1]
            last_excute_timestamp = int(last_excute_timestamp)
            last_excute_timestamp = last_excute_timestamp + 1
            current_date = datetime.now()
            current_timestamp = datetime.timestamp(current_date)
            _date = datetime.utcfromtimestamp(int(current_timestamp)).strftime("%Y-%m-%d")
            _time = datetime.utcfromtimestamp(int(current_timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, " " * 5, self.tickers, '#' * 20)
            if self.stock_data_engines[self.tickers[0]].get_data_by_range(
                    [last_excute_timestamp, current_timestamp]) is None:
                print("No new data")
            else:
                print("Have new data")
                timestamps_1 = \
                    self.stock_data_engines[self.tickers[0]].get_data_by_range(
                        [last_excute_timestamp, current_timestamp])[
                        'timestamp']
                timestamps_2 = \
                    self.stock_data_engines[self.tickers[1]].get_data_by_range(
                        [last_excute_timestamp, current_timestamp])[
                        'timestamp']
                timestamps = np.intersect1d(timestamps_1, timestamps_2)
                for timestamp in timestamps:
                    self.run_realtime(timestamp)
                self.backtest.end_date = current_date
                # self.plot_all_file_graph()

    def run_realtime(self, timestamp):  # run realtime
        _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
        _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
        print('#' * 20, _date, ":", _time, " " * 5, self.tickers, '#' * 20)
        if self.dividend_agent.check_div(timestamp):
            portfolio = self.portfolio_data_engine.get_portfolio()
            total_dividend = self.dividend_agent.distribute_div(timestamp, portfolio)
            if total_dividend != 0:
                self.portfolio_data_engine.deposit_dividend(total_dividend, timestamp)

        stock_data_dict = {}
        sim_meta_data = {}
        pct_change_dict = {}
        for ticker in self.tickers:
            pct_change_dict.update({ticker: {}})
        for ticker in self.tickers:
            ticker_data = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(timestamp)
            if ticker_data is not None:
                ticker_engine = self.stock_data_engines[ticker]
                ticker_items = ticker_engine.get_ticker_item_by_timestamp(timestamp)
                self.indicators[ticker].append_into_df(ticker_items)
                pct_change_dict[ticker].update({1: self.indicators[ticker].get_pct_change(1, 'open', timestamp)})
                pct_change_dict[ticker].update({3: self.indicators[ticker].get_pct_change(3, 'open', timestamp)})
                pct_change_dict[ticker].update({6: self.indicators[ticker].get_pct_change(6, 'open', timestamp)})
                sim_meta_data.update({ticker: ticker_data})
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

        action_msgs = self.algorithm.run(stock_data_dict, self.bond)
        action_record = []
        if action_msgs is None:
            self.sim_agent.append_run_data_to_db(timestamp, self.sim_agent.portfolio_data_engine.get_account_snapshot(),
                                                 action_record, sim_meta_data,
                                                 stock_data_dict)
            if self.store_mongoDB:
                print("(*&^%$#$%^&*()(*&^%$#$%^&*(")
                p = Write_Mongodb()
                for file in os.listdir(self.backtest_data_directory):
                    if file.decode().endswith("csv"):
                        csv_path = Path(self.run_file_dir, file.decode())
                        a = pd.read_csv(csv_path)
                        spec = file.decode().split('.csv')
                        p.write_new_backtest_result(strategy_name=self.table_name + '_' + spec[0],
                                                    run_df=a)
            return
        for action_msg in action_msgs:
            action = action_msg.action_enum
            if action == IBAction.SELL_MKT_ORDER:
                temp_action_record = self.trade_agent.place_sell_stock_mkt_order(action_msg.args_dict.get("ticker"),
                                                                            action_msg.args_dict.get("position_sell"),
                                                                            {"timestamp": action_msg.timestamp})
                action_record.append(temp_action_record)
        for action_msg in action_msgs:
            action = action_msg.action_enum
            if action == IBAction.BUY_MKT_ORDER:
                temp_action_record = self.trade_agent.place_buy_stock_mkt_order(action_msg.args_dict.get("ticker"),
                                                                           action_msg.args_dict.get(
                                                                               "position_purchase"),
                                                                           {"timestamp": action_msg.timestamp})
                action_record.append(temp_action_record)
        self.sim_agent.append_run_data_to_db(timestamp, self.sim_agent.portfolio_data_engine.get_account_snapshot(),
                                        action_record, sim_meta_data,
                                        stock_data_dict)
        if self.store_mongoDB:
            print("(*&^%$#$%^&*()(*&^%$#$%^&*(")
            p = Write_Mongodb()
            for file in os.listdir(self.backtest_data_directory):
                if file.decode().endswith("csv"):
                    csv_path = Path(self.run_file_dir, file.decode())
                    a = pd.read_csv(csv_path)
                    spec = file.decode().split('.csv')
                    p.write_new_backtest_result(strategy_name=self.table_name + '_' + spec[0],
                                                run_df = a)
    def plot_all_file_graph(self):
        print("plot_graph")
        graph_plotting_engine.plot_all_file_graph_png(f"{self.backtest.run_file_dir}", "date", "NetLiquidation",
                                                      f"{self.path}/{self.table_name}/graph")


def main():
    tickers = ["SPY", "MSFT"]
    initial_amount = 10000
    start_date = datetime(2020, 1, 1)  # YYMMDD
    data_freq = "one_day"
    user_id = 0
    cal_stat = True
    db_mode = {"dynamo_db": False, "local": True}
    bond = "TIP"
    execute_period = "Monthly"

    realtime_backtest = realtime(tickers, bond, initial_amount, start_date, cal_stat,
                                 data_freq, user_id, db_mode, execute_period)
    while True:
        realtime_backtest.run()
        time.sleep(60)


if __name__ == "__main__":
    main()