import numpy as np
import pandas as pd
import yfinance as yf
import pathlib
from datetime import datetime
from algo.portfolio_rebalance.backtest import backtest as portfolio_rebalance_backtest
from object.action_data import IBAction, IBActionsTuple


class realtime:
    def __init__(self, list_of_tickers, initial_amount, start_date, cal_stat, data_freq, user_id,
                 db_mode, acceptance_range, list_of_rebalance_ratios, execute_period):
        self.algorithm = None
        self.dividend_agent = None
        self.sim_agent = None
        self.trade_agent = None
        self.portfolio_data_engine = None
        self.acc_data = None
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/realtime"

        self.table_info = {"mode": "backtest", "strategy_name": "portfolio_rebalance", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.user_id = user_id
        self.list_of_tickers = list_of_tickers
        self.initial_amount = initial_amount
        self.start_timestamp = datetime.timestamp(start_date)
        self.cal_stat = cal_stat
        self.data_freq = data_freq
        self.db_mode = db_mode
        self.rebalance_ratio = list_of_rebalance_ratios
        self.start_date = start_date
        self.tickers = []
        self.backtest = None
        self.now = datetime.now()
        self.acceptance_range = acceptance_range
        self.execute_period = execute_period

    def init_backtest(self, user_id, acceptance_range, store_mongoDB, strategy_initial, video_link,
                      documents_link, tags_array, subscribers_num,
                      rating_dict, margin_ratio, trader_name):
        self.now = datetime.now()

        end_date = self.now.date()
        self.backtest = portfolio_rebalance_backtest(self.list_of_tickers, self.initial_amount,
                                                     self.start_date, end_date, True, self.data_freq,
                                                     user_id, self.db_mode, True, acceptance_range,
                                                     self.rebalance_ratio, store_mongoDB, strategy_initial,
                                                     video_link, documents_link, tags_array, subscribers_num,
                                                     rating_dict, margin_ratio, trader_name)
        self.backtest.loop_through_param()

        pass

    def check_exec(self, execute_period):  # check whether it should do any execution in a specify period
        datetime_obj = datetime.now()
        if self.backtest.end_date == None:
            # self.backtest.end_date = datetime_obj
            return True
        else:
            # relative_delta = kwargs.pop("relative_delta")
            if execute_period == "Daily":
                if datetime_obj.day != self.backtest.end_date.day and datetime_obj > self.backtest.end_date:
                    # self.last_exec_datetime_obj = datetime_obj
                    # print(
                    # f"check_exec: True. last_exec_datetime_obj.day={self.last_exec_datetime_obj.day}; datetime_obj.day={datetime_obj.day}")
                    return True
                else:
                    # print(
                    # f"check_exec: False. last_exec_datetime_obj.day={self.last_exec_datetime_obj.day}; datetime_obj.day={datetime_obj.day}")
                    return False

            elif execute_period == "Monthly":
                if datetime_obj.month != self.backtest.end_date.month and datetime_obj > self.backtest.end_date:
                    # print(
                    # f"check_exec: True. last_exec_datetime_obj.month={self.last_exec_datetime_obj.month}; datetime_obj.month={datetime_obj.month}")
                    # self.backtest.end_date = datetime_obj
                    return True
                else:
                    # print(
                    # f"check_exec: False. last_exec_datetime_obj.month={self.last_exec_datetime_obj.month}; datetime_obj.month={datetime_obj.month}")
                    return False

    def run(self):  # decide run realtime or backtest
        self.acc_data = self.backtest.acc_data
        self.portfolio_data_engine = self.backtest.portfolio_data_engine
        self.trade_agent = self.backtest.trade_agent
        self.sim_agent = self.backtest.sim_agent
        self.dividend_agent = self.backtest.dividend_agent
        self.algorithm = self.backtest.algorithm
        last_excute_day = self.backtest.end_date
        current_date = datetime.now()
        timestamp = datetime.timestamp(current_date)
        if self.dividend_agent.check_div(timestamp):
            portfolio = self.portfolio_data_engine.get_portfolio()
            total_dividend = self.dividend_agent.distribute_div(timestamp, portfolio)
            if total_dividend != 0:
                self.portfolio_data_engine.deposit_dividend(total_dividend, timestamp)

        if self.data_freq == "one_month":
            if current_date.year == last_excute_day.year and current_date.month == last_excute_day.month:
                self.run_realtime(timestamp)
            else:
                self.run_backtest(timestamp)
        elif self.data_freq == "one_day":
            if current_date.year == last_excute_day.year and current_date.month == last_excute_day.month and current_date.day == last_excute_day.day:
                self.run_realtime(timestamp)
            else:
                self.run_backtest(timestamp)
        elif self.data_freq == "one_minute":
            if current_date.year == last_excute_day.year and current_date.month == last_excute_day.month and current_date.day == last_excute_day.day and current_date.hour == last_excute_day.hour and current_date.minute == last_excute_day.minute:
                self.run_realtime(timestamp)
            else:
                self.run_backtest(timestamp)

    def run_backtest(self, timestamp):  # run backtest
        pass

    def run_realtime(self, timestamp):  # run realtime
        if self.data_freq == "one_day":
            stock_data_dict = {}
            sim_meta_data = {}

            for ticker in self.tickers:
                temp_ticker_data = yf.download(tickers=ticker, period='1d', interval='1d')
                if temp_ticker_data != None:
                    ticker_open_price = temp_ticker_data.get("open")
                    stock_data_dict.update({ticker: {'last': ticker_open_price}})
                    sim_meta_data.update({ticker: temp_ticker_data})
            orig_account_snapshot_dict = self.sim_agent.portfolio_data_engine.get_account_snapshot()
            action_msgs = self.algorithm.run(stock_data_dict, timestamp)
            action_record = []
            for action_msg in action_msgs:
                action = action_msg.action_enum
                if action == IBAction.SELL_MKT_ORDER:
                    temp_action_record = self.trade_agent.place_sell_stock_mkt_order(action_msg.args_dict.get("ticker"),
                                                                                     action_msg.args_dict.get(
                                                                                         "position_sell"),
                                                                                     {
                                                                                         "timestamp": action_msg.timestamp})
                    action_record.append(temp_action_record)
            for action_msg in action_msgs:
                action = action_msg.action_enum
                if action == IBAction.BUY_MKT_ORDER:
                    temp_action_record = self.trade_agent.place_buy_stock_mkt_order(action_msg.args_dict.get("ticker"),
                                                                                    action_msg.args_dict.get(
                                                                                        "position_purchase"),
                                                                                    {"timestamp": action_msg.timestamp})
                    action_record.append(temp_action_record)
            self.sim_agent.append_run_data_to_db(timestamp, orig_account_snapshot_dict, action_record, sim_meta_data,
                                                 stock_data_dict)


def main():
    tickers = ["M", "MSFT"]
    rebalance_ratio = [[50, 50]]
    initial_amount = 10000
    start_date = datetime(2010, 1, 1)  # YYMMDD
    data_freq = "one_day"
    user_id = 0
    cal_stat = True
    db_mode = {"dynamo_db": False, "local": True}
    acceptance_range = 0
    execute_period = "Monthly"

    realtime_backtest = realtime(tickers, initial_amount, start_date, cal_stat,
                                 data_freq, user_id, db_mode, acceptance_range,
                                 rebalance_ratio, execute_period)
    realtime_backtest.init_backtest(realtime_backtest.user_id, realtime_backtest.acceptance_range,
                                    store_mongoDB=True,
                                    strategy_initial='this is 20 80 m and msft portfolio',
                                    video_link='https://www.youtube.com',
                                    documents_link='https://google.com',
                                    tags_array=None,
                                    subscribers_num=3,
                                    rating_dict=None,
                                    margin_ratio=3.24,
                                    trader_name='Fai'
                                    )
    while True:
        realtime_backtest.run()


if __name__ == "__main__":
    main()
