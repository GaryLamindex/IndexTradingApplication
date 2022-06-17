import datetime as dt
import pathlib
import heapq

from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from engine.backtest_engine.stock_data_io_engine import crypto_local_engine
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from object.backtest_acc_data import backtest_acc_data
from algo.momentum_strategy.algorithm import momentum_strategy
from enum import Enum


class Action(Enum):
    # market order
    BUY_MKT_ORDER = 1
    SELL_MKT_ORDER = 2
    CLOSE_ALL = 3

    # limit order
    BUY_LMT_ORDER = 4
    SELL_LMT_ORDER = 5

class backtest:
    def __init__(self, tickers, initial_amount, start_date, end_date, cal_stat, user_id,
                 period_dict):
        self.table_info = {"mode": "backtest", "strategy_name": "momentum_strategy", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.periods_dict = period_dict
        self.tickers = tickers
        self.initial_amount = initial_amount
        self.start_timestamp = int(start_date.timestamp())
        self.end_timestamp = int(end_date.timestamp())
        self.cal_stat = cal_stat
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"

        self.crypto_data_engines = {}
        for ticker in tickers:
            self.crypto_data_engines[ticker] = crypto_local_engine(ticker)

        # self.pending_actions is a heap queue (or priority queue)
        # only the tuples of the following form should be stored
        # (execution timestamp, Action Enum, args dict for the action)
        self.pending_actions = []

    def loop_through_params(self):
        start = self.periods_dict.get('start')
        end = self.periods_dict.get('end')
        step = self.periods_dict.get('step')

        for period in range(start, end, step):

            backtest_spec = {"period": period}
            spec_str = ""
            for k, v in backtest_spec.items():
                spec_str = f"{spec_str}{str(v)}_{str(k)}_"

            acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                         self.table_name, spec_str)
            portfolio_data_engine = backtest_portfolio_data_engine(acc_data, self.tickers)
            trade_agent = backtest_trade_engine(acc_data, self.crypto_data_engines, portfolio_data_engine)
            sim_agent = simulation_agent(backtest_spec, self.table_info, False, portfolio_data_engine, self.tickers)

            algorithm = momentum_strategy(portfolio_data_engine)
            self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, algorithm,
                               period, portfolio_data_engine, sim_agent, trade_agent)
            print('finished backtest:', backtest_spec)

            if self.cal_stat:
                self.cal_all_file_return()

    def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm,
                      period, portfolio_data_engine, sim_agent, trade_agent):
        if len(self.tickers) < 2:
            print('This strategy does not work for < 2 tickers')
            exit(0)

        print('start backtest')
        print('Fetch data')

        portfolio_data_engine.deposit_cash(initial_amount, start_timestamp)

        step = 86400  # 1 day timestamp

        for timestamp in range(start_timestamp, end_timestamp, step):
            _date = dt.datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            _time = dt.datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, '#' * 20)
            self.run(timestamp, algorithm, period, sim_agent, trade_agent, portfolio_data_engine)

    def run(self, timestamp, algorithm, period, sim_agent, trade_agent, portfolio_data_engine):
        pct_change_dict = {}
        price_dict = {}
        for ticker in self.tickers:
            ticker_engine = self.crypto_data_engines[ticker]
            if ticker_engine.has_started_at_timestamp(timestamp):
                price_dict.update({ticker: ticker_engine.get_field_by_timestamp(timestamp, 'price')})
                pct_change_dict.update({ticker: ticker_engine.get_pct_change_by_timestamp(period, timestamp)})

        # algorithm.run() should return proper format of tuples in a list for self.pending_actions
        heapq.heappush(self.pending_actions, algorithm.run(price_dict, pct_change_dict, timestamp))

        while self.pending_actions[0][0] == timestamp:
            cur_action = self.pending_actions[0][1]
            func_params = self.pending_actions[0][2]

            if cur_action == Action.BUY_MKT_ORDER:
                ticker = func_params['ticker']
                last = self.crypto_data_engines[ticker].get_data_by_timestamp(timestamp)
                trade_agent.place_buy_stock_mkt_order(ticker,
                                                      func_params['position_purchase'],
                                                      {'timestamp': timestamp, 'last': last})
            elif cur_action == Action.CLOSE_ALL:
                portfolio = portfolio_data_engine.acc_data.portfolio
                for p in portfolio:
                    ticker = p['ticker']
                    cur_position = p['position']
                    if cur_position >= 1:
                        open_price = self.crypto_data_engines[ticker].get_data_by_timestamp(timestamp)
                        trade_agent.place_sell_stock_mkt_order(ticker, cur_position,
                                                               {'timestamp': timestamp, 'open': open_price})
            heapq.heappop(self.pending_actions)

    def cal_all_file_return(self):
        pass
