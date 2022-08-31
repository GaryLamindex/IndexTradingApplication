import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from algo.random_forest.indicator import Indicator
import quandl
from object.action_data import IBAction, IBActionsTuple

class RandomForest:
    def __init__(self, trade_agent, portfolio_agent):
        self.account_snapshot = {}
        self.portfolio = []
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.last_exec_datetime_obj = None
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("NetLiquidation")
        self.buy = ""
        self.optimal_weight = {}
        self.action_msgs = []

    def run(self, price_dict, all_indice, rate, timestamp):
        if not self.trade_agent.market_opened():
            return

        self.portfolio_agent.update_stock_price_and_portfolio_data(price_dict)
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.portfolio = self.portfolio_agent.get_portfolio()
        self.total_market_value = self.account_snapshot.get("NetLiquidation")

        return_dict = {}

        """
        How confident you are in the algorithm.
        If positive, higher weight on stocks w/ higher expected return;
        if 0, equal weight on all stocks (and cash);
        if negative, higher weight on stocks w/ lower expected return.
        In theory can take any real numbers, but in practice choose a number in the range [-10000, 10000].
        """
        confidence = 1000

        for ticker in price_dict.keys():
            indicator = Indicator(all_indice[ticker], rate)
            indicator.get_samples()
            X, y = indicator.dataset.drop("Return", axis=1), indicator.dataset["Return"]
            if np.inf in X.values:
                return_dict[ticker] = -np.inf
                continue
            regr = RandomForestRegressor(n_estimators=100,
                                         max_features=1 / 3,
                                         min_samples_leaf=0.02,
                                         random_state=23571113,
                                         max_samples=0.75)
            regr.fit(X, y)
            return_dict[ticker] = regr.predict(indicator.last_dataset.reshape(1, -1))[0]
        print(return_dict)
        # total_return = sum([math.exp(ret) for ret in return_dict.values() if ret > 0])
        # self.optimal_weight = {ticker: (math.exp(ret) / total_return if ret > 0 else 0) for ticker, ret in return_dict.items()}
        total_return = sum([math.exp(confidence * ret) for ret in return_dict.values()]) + 1
        self.optimal_weight = {ticker: (math.exp(confidence * ret) / total_return) for ticker, ret in return_dict.items()}
        self.action_msgs = []

        for ticker_data in self.portfolio:
            ticker_name = ticker_data.get("ticker")
            ticker_pos = ticker_data.get("position")
            if ticker_name in price_dict.keys():
                price = all_indice[ticker_name]["Close"].iloc[-1]
                print("Weight:", self.optimal_weight[ticker_name])
                target_pos = int(self.optimal_weight[ticker_name] * self.total_market_value / price)
                print(f"{ticker_name} Current Position: {ticker_pos}; Target Position: {target_pos}; Market Value: {self.total_market_value}")
                pos_change = target_pos - ticker_pos
                if pos_change < 0:
                    action_msg = IBActionsTuple(timestamp, IBAction.SELL_MKT_ORDER,
                                                {'ticker': ticker_name, 'position_sell': -pos_change})
                    self.action_msgs.append(action_msg)
                elif pos_change > 0:
                    action_msg = IBActionsTuple(timestamp, IBAction.BUY_MKT_ORDER,
                                                {'ticker': ticker_name, 'position_purchase': pos_change})
                    self.action_msgs.append(action_msg)

        return self.action_msgs.copy()

    def check_exec(self, timestamp, **kwargs):
        datetime_obj = datetime.utcfromtimestamp(timestamp)
        if self.last_exec_datetime_obj is None:
            self.last_exec_datetime_obj = datetime_obj
            return True
        else:
            freq = kwargs.pop("freq")
            relative_delta = kwargs.pop("relative_delta")
            if freq == "Weekly":
                # next_exec_datetime_obj = self.last_exec_datetime_obj + relativedelta(days=+relative_delta)
                if datetime_obj > self.last_exec_datetime_obj + timedelta(days=6):
                    self.last_exec_datetime_obj = self.last_exec_datetime_obj + timedelta(days=7)
                    # print(
                    # f"check_exec: True. last_exec_datetime_obj.day={self.last_exec_datetime_obj.day}; datetime_obj.day={datetime_obj.day}")
                    return True
                else:
                    # print(
                    # f"check_exec: False. last_exec_datetime_obj.day={self.last_exec_datetime_obj.day}; datetime_obj.day={datetime_obj.day}")
                    return False

            elif freq == "Monthly":
                # next_exec_datetime_obj = self.last_exec_datetime_obj + relativedelta(months=+relative_delta)
                if datetime_obj.month != self.last_exec_datetime_obj.month and datetime_obj > self.last_exec_datetime_obj:
                    # print(
                    # f"check_exec: True. last_exec_datetime_obj.month={self.last_exec_datetime_obj.month}; datetime_obj.month={datetime_obj.month}")
                    self.last_exec_datetime_obj = datetime_obj
                    return True
                else:
                    # print(
                    # f"check_exec: False. last_exec_datetime_obj.month={self.last_exec_datetime_obj.month}; datetime_obj.month={datetime_obj.month}")
                    return False
