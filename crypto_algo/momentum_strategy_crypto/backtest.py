import datetime as dt
import pathlib
import heapq
import os
from pathlib import Path
from enum import Enum

from crypto_algo.momentum_strategy_crypto.algorithm import momentum_strategy
from engine.crypto_engine.crypto_data_io_engine import crypto_local_engine
from engine.simulation_engine.simulation_agent import simulation_agent
from engine.crypto_engine.crypto_portfolio_data_engine import crypto_portfolio_data_engine
from engine.crypto_engine.crypto_trade_engine import crypto_trade_engine
from object.crypto_acc_data import crypto_acc_data


class Action(Enum):
    # market order
    BUY_MKT_ORDER = 1
    SELL_MKT_ORDER = 2
    CLOSE_POSITION = 3
    CLOSE_ALL = 4

    # limit order
    BUY_LMT_ORDER = 5
    SELL_LMT_ORDER = 6


class ActionsTuple:
    def __init__(self, timestamp, action_enum, args_dict):
        self.timestamp = timestamp
        self.action_enum = action_enum
        self.args_dict = args_dict

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __getitem__(self, item):
        if item == 0:
            return self.timestamp
        elif item == 1:
            return self.action_enum
        elif item == 2:
            return self.args_dict


class backtest:
    def __init__(self, tickers, initial_amount, start_date, end_date, cal_stat, user_id,
                 period_dict, db_mode):
        self.table_info = {"mode": "backtest", "strategy_name": "momentum_strategy_crypto", "user_id": user_id}
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))
        self.periods_dict = period_dict
        self.tickers = tickers
        self.initial_amount = initial_amount
        self.start_timestamp = int(start_date.timestamp())
        self.end_timestamp = int(end_date.timestamp())
        self.cal_stat = cal_stat
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"
        self.db_mode = db_mode

        self.crypto_data_engines = {}
        for ticker in tickers:
            self.crypto_data_engines[ticker] = crypto_local_engine(ticker)

        # self.pending_actions is a heap queue (or priority queue)
        # only ActionsTuple should be stored
        self.pending_actions = []

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

    def loop_through_params(self):
        start = self.periods_dict.get('start')
        end = self.periods_dict.get('end')
        step = self.periods_dict.get('step')

        for period in range(start, end, step):

            backtest_spec = {"period": period}
            spec_str = ""
            for k, v in backtest_spec.items():
                spec_str = f"{spec_str}{str(v)}_{str(k)}_"

            run_file = self.run_file_dir + spec_str + '.csv'
            if os.path.exists(run_file):
                os.remove(Path(run_file))
            graph_file = self.graph_dir + spec_str + '.png'
            if os.path.exists(graph_file):
                os.remove(Path(graph_file))

            acc_data = crypto_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                         self.table_name, spec_str)
            portfolio_data_engine = crypto_portfolio_data_engine(acc_data, self.tickers)
            trade_agent = crypto_trade_engine(acc_data, self.crypto_data_engines, portfolio_data_engine)
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

        portfolio_data_engine.deposit_cash('funding', initial_amount, start_timestamp)

        step = 86400  # 1 day timestamp

        for timestamp in range(start_timestamp, end_timestamp, step):
            _date = dt.datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            _time = dt.datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
            print('#' * 20, _date, ":", _time, '#' * 20)
            self.run(timestamp, algorithm, period, sim_agent, trade_agent, portfolio_data_engine)

    def run(self, timestamp, algorithm, period, sim_agent, trade_agent, portfolio_data_engine):
        if timestamp == 1602460800:
            a = 0
            b = 1
            pass
        pct_change_dict = {}
        price_dict = {}
        sim_meta_data = {}
        stock_data_dict = {}
        for ticker in self.tickers:
            ticker_engine = self.crypto_data_engines[ticker]
            price = ticker_engine.get_field_by_timestamp(timestamp, 'price')
            if price is not None:
                stock_data_dict.update({ticker: {'last': price}})
                price_dict.update({ticker: price})
                pct_change_dict.update({ticker: ticker_engine.get_pct_change_by_timestamp(period, timestamp)})
                sim_meta_data.update({ticker: ticker_engine.get_ticker_item_by_timestamp(timestamp)})
            else:
                return

        # algorithm.run() should return proper format of tuples in a list for self.pending_actions
        temp_actions = algorithm.run(price_dict, pct_change_dict, timestamp)
        for a in temp_actions:
            heapq.heappush(self.pending_actions, a)

        while len(self.pending_actions) != 0 and self.pending_actions[0][0] >= timestamp:
            cur_action = self.pending_actions[0][1]
            func_params = self.pending_actions[0][2]
            action_msg = None

            if cur_action == Action.BUY_MKT_ORDER:
                ticker = func_params['ticker']
                last = self.crypto_data_engines[ticker].get_data_by_timestamp(timestamp)['price'].item()
                action_msg = trade_agent.place_buy_crypto_mkt_order(ticker,
                                                                    func_params['position_purchase'],
                                                                    timestamp, last)
            elif cur_action == Action.CLOSE_ALL:
                portfolio = portfolio_data_engine.acc_data.portfolio
                for p in portfolio:
                    ticker = p['ticker']
                    cur_position = p['available']
                    if cur_position > 0:
                        open_price = self.crypto_data_engines[ticker].get_data_by_timestamp(timestamp)['price'].item()
                        action_msg = trade_agent.place_sell_crypto_mkt_order(ticker, cur_position,
                                                                             timestamp, open_price)
                    portfolio_data_engine.acc_data.clear_portfolio_item()
            elif cur_action == Action.CLOSE_POSITION:
                ticker = func_params['ticker']
                ticker_item = portfolio_data_engine.acc_data.check_if_ticker_exist_in_portfolio(ticker)
                cur_position = ticker_item['available']
                open_price = self.crypto_data_engines[ticker].get_data_by_timestamp(timestamp)['price'].item()
                action_msg = trade_agent.place_sell_crypto_mkt_order(ticker, cur_position,
                                                                     timestamp, open_price)
                portfolio_data_engine.acc_data.remove_portfolio_item(ticker)
            if action_msg is not None:
                sim_agent.append_run_data_to_db(timestamp, portfolio_data_engine.get_overview(),
                                                [action_msg], sim_meta_data, stock_data_dict)
            heapq.heappop(self.pending_actions)

    def cal_all_file_return(self):
        pass
