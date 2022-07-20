import os
import pathlib
from datetime import datetime
from os import listdir
from pathlib import Path

import pandas as pd
from pandas.core import series

from algo.rebalance_margin_wif_maintainance_margin.algorithm import rebalance_margin_wif_maintainance_margin
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from engine.backtest_engine.stock_data_io_engine import local_engine
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.simulation_engine import sim_data_io_engine
from engine.aws_engine.dynamo_db_engine import dynamo_db_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from engine.simulation_engine.statistic_engine import statistic_engine
from object.backtest_acc_data import backtest_acc_data


class backtest(object):
    path = ""
    data_freq = ""
    tickers = []
    initial_amount = 0
    start_timestamp = None
    end_date = None
    cal_stat = True
    wipe_previous_sim_data = False
    rabalance_dict = {}
    maintain_dict = {}
    max_drawdown_ratio_dict = {}
    purchase_exliq_ratio_dict= {}
    algo = None
    dynamo_db = None
    table_info = {}
    table_name = ""

    run_file_dir = ""
    stats_data_dir = ""
    acc_data_dir = ""
    transact_data_dir = ""

    stock_data_engines = {}

    # maximum ONLY 2 tickers at a time !!!
    def __init__(self, tickers, initial_amount, start_date, end_date, cal_stat, rabalance_dict, maintain_dict, purchase_exliq_ratio_dict, data_freq, user_id, db_mode):
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"

        self.table_info = {"mode": "backtest", "strategy_name": "rebalance_margin_wif_maintainance_margin", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(self.table_info.get("user_id"))
        self.tickers = tickers
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.end_timestamp = datetime.timestamp(end_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq

        self.rabalance_dict = rabalance_dict
        self.maintain_dict = maintain_dict
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

            list_of_run_files = listdir(self.run_file_dir)
            list_of_stats_data = listdir(self.stats_data_dir)
            list_of_acc_data = listdir(self.acc_data_dir)
            list_of_transact_data = listdir(self.transact_data_dir)

            for file in list_of_run_files:
                os.remove(Path(f"{self.run_file_dir}/{file}"))
            for file in list_of_stats_data:
                os.remove(Path(f"{self.stats_data_dir}/{file}"))
            for file in list_of_acc_data:
                os.remove(Path(f"{self.acc_data_dir}/{file}"))
            for file in list_of_transact_data:
                os.remove(Path(f"{self.transact_data_dir}/{file}"))

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

        purchase_exliq_ratio_start = self.purchase_exliq_ratio_dict.get("start")
        purchase_exliq_ratio_end = self.purchase_exliq_ratio_dict.get("end")
        purchase_exliq_ratio_step = self.purchase_exliq_ratio_dict.get("step")

        # loop through all the rebalance requirement
        for rebalance in range(rebalance_start, rebalance_end, rebalance_step):
            for maintain in range(maintain_start, maintain_end, maintain_step):
                for purchase_exliq_ratio in range(purchase_exliq_ratio_start, purchase_exliq_ratio_end, purchase_exliq_ratio_step):

                    if (maintain > rebalance):
                        continue

                    rebalance_margin = rebalance/1000
                    maintain_margin = maintain/1000
                    purchase_exliq = purchase_exliq_ratio/100
                    acceptance_range = 0

                    backtest_spec = {"rebalance_margin":rebalance_margin, "maintain_margin":maintain_margin,  "purchase_exliq":purchase_exliq}
                    spec_str = ""
                    for k, v in backtest_spec.items():
                        spec_str = f"{spec_str}{str(v)}_{str(k)}_"

                    acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"), self.table_name, spec_str)
                    portfolio_data_engine = backtest_portfolio_data_engine(acc_data)
                    trade_agent = backtest_trade_engine(acc_data, self.stock_data_engines, portfolio_data_engine)
                    sim_agent = simulation_agent(backtest_spec, self.table_info, False, portfolio_data_engine, self.tickers)

                    algorithm = rebalance_margin_wif_maintainance_margin(trade_agent, portfolio_data_engine, self.tickers, acceptance_range, rebalance_margin)
                    self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, algorithm, portfolio_data_engine, sim_agent)
                    print("Finished Backtest:", backtest_spec)

                    if(self.cal_stat == True):
                        print("start backtest")
                        self.cal_all_file_return()

    def cal_all_file_return(self):
        sim_data_offline_engine = sim_data_io_engine.offline_engine(self.run_file_dir)
        backtest_data_directory = os.fsencode(self.run_file_dir)
        data_list = []
        for file in os.listdir(backtest_data_directory):
            if file.decode().endswith("csv"):

                file_name = file.decode().split(".csv")[0]
                stat_engine = statistic_engine(sim_data_offline_engine)
                sharpe_dict = stat_engine.get_sharpe_data(spec=file_name)
                inception_sharpe = sharpe_dict.get("inception")
                _1_yr_sharpe = sharpe_dict.get("1y")
                _3_yr_sharpe = sharpe_dict.get("3y")
                _5_yr_sharpe = sharpe_dict.get("5y")
                _ytd_sharpe = sharpe_dict.get("ytd")

                return_dict = stat_engine.get_return_data(spec=file_name)
                inception_return = return_dict.get("inception")
                _1_yr_return = return_dict.get("1y")
                _3_yr_return = return_dict.get("3y")
                _5_yr_return = return_dict.get("5y")
                _ytd_return = return_dict.get("ytd")

                all_file_stats_row = {
                    "Backtest Spec":file_name, 'YTD Return':_ytd_return, '1 Yr Return':_1_yr_return, "3 Yr Return":_3_yr_return, "5 Yr Return":_5_yr_return,
                    "Since Inception Return":inception_return, "Since Inception Sharpe":inception_sharpe, "YTD Sharpe":_ytd_sharpe,
                    "1 Yr Sharpe":_1_yr_sharpe, "3 Yr Sharpe":_3_yr_sharpe,"5 Yr Sharpe":_5_yr_sharpe
                }
                # _additional_data = self.cal_additional_data(file_name)
                # data_list.append(all_file_stats_row | _additional_data)

        col = ['Backtest Spec', 'YTD Return', '1 Yr Return', "3 Yr Return", "5 Yr Return",
             "Since Inception", "Since Inception Sharpe", "YTD Sharpe", "1 Yr Sharpe", "3 Yr Sharpe",
             "5 Yr Sharpe", "min(exliq/mkt value)"]
        df = pd.DataFrame(data_list, columns=col)
        df.fillna(0)
        print(f"{self.path}/stats_data/{self.table_name}.csv")
        df.to_csv(f"{self.path}/stats_data/{self.table_name}.csv")
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

    def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm, portfolio_data_engine, sim_agent):
        # connect to downloaded ib data to get price data
        print("start backtest")
        row = 0
        print("Fetch data")
        stock_data_dict = {}
        if len(self.tickers) == 1:
            timestamps = self.stock_data_engines[self.tickers[0]].get_data_by_range([start_timestamp, end_timestamp])['timestamp']
        elif len(self.tickers) == 2:
            series_1 = self.stock_data_engines[self.tickers[0]].get_data_by_range([start_timestamp, end_timestamp])['timestamp']
            series_2 = self.stock_data_engines[self.tickers[1]].get_data_by_range([start_timestamp, end_timestamp])['timestamp']
            timestamps = self.stock_data_engines[self.tickers[0]].get_union_timestamps(series_1,series_2)

        for timestamp in timestamps:
            timestamp = int(timestamp)
            print("timestamp:", timestamp)
            stock_data_dict = {}
            stock_data_dict.update({"timestamp": timestamp})

            for ticker in self.tickers:
                # get stock data from historical data
                print("timestamp:",timestamp,"; ticker:",ticker)
                ticker_data = self.stock_data_engines[ticker].get_ticker_item_by_timestamp(timestamp)
                if ticker_data != None:
                    ticker_open_price = ticker_data.get("open")
                    print("ticker_open_price",ticker_open_price)
                    stock_data_dict.update({ticker: {'last':ticker_open_price}})
                    print("stock_data_dict", stock_data_dict)

                # ticker_div = ticker_stock_data.get(Query().timestamp == timestamp).get(ticker + ' div amount')
                # div_data_dict.update({ticker + ' div amount': ticker_div})


            _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, '#' * 20)

            if row == 0:
                # input initial cash
                portfolio_data_engine.deposit_cash(initial_amount, timestamp)
                row += 1

            # input database and historical data into algo
            action_msgs = algorithm.run(timestamp, )
            # TODO: store action msgs

            # TODO: check and exec action msgs

            sim_meta_data = {}

            if algorithm.overview["GrossPositionValue"] != 0:
                ExcessLiquidity_to_GrossPositionValue = algorithm.overview["ExcessLiquidity"] / algorithm.overview["GrossPositionValue"]
            else:
                ExcessLiquidity_to_GrossPositionValue = 0

            sim_meta_data.update({"ExcessLiquidity_to_GrossPositionValue":ExcessLiquidity_to_GrossPositionValue})
            # for ticker in self.tickers:
            #     sim_meta_data.update({ticker: {"max_stock_price":algorithm.max_stock_price[ticker]}})
            #     sim_meta_data[ticker]["benchmark_drawdown_price"] = algorithm.benchmark_drawdown_price[ticker]
            #     sim_meta_data[ticker]["liq_sold_qty_dict"] = algorithm.liq_sold_qty_dict[ticker]


            sim_agent.append_run_data_to_db(timestamp, action_msgs, sim_meta_data, stock_data_dict)
            sim_agent.write_transaction_record(action_msgs)
            # sim_agent.write_acc_data()