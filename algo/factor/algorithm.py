from object.backtest_acc_data import backtest_acc_data
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
from object.action_data import IBAction, IBActionsTuple
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from indicator import Indicator

class Factor:

    def __init__(self, trade_agent, portfolio_agent):
        self.account_snapshot = {}
        self.portfolio = []
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.last_exec_datetime_obj = None
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("NetLiquidation")
        self.buy = ""
        self.optimal_weight = pd.Series([])
        self.action_msgs = []

    def run(self, price_dict, all_indice_df, timestamp):
        # remove price_dict param if possible
        if not self.trade_agent.market_opened():
            return

        self.portfolio_agent.update_stock_price_and_portfolio_data(price_dict)
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.portfolio = self.portfolio_agent.get_portfolio()
        self.total_market_value = self.account_snapshot.get("NetLiquidation")

        indicators = Indicator(all_indice_df)
        expected_return, expected_cov = indicators.expected_return, indicators.expected_cov
        lb = 0
        ub = 1

        def MV(w, cov_mat):
            return np.dot(w, np.dot(cov_mat, w.T))

        n = len(expected_cov.columns)
        muRange = np.arange(0.02, 0.09, 0.002)
        volRange = np.zeros(len(muRange))
        #omega = expected_cov.cov()

        wgt = {}

        for i in range(len(muRange)):
            mu = muRange[i]
            wgt[mu] = []
            x_0 = np.ones(n) / n
            bndsa = ((lb, ub),)
            for j in range(1, n):
                bndsa = bndsa + ((lb, ub),)
            consTR = ({'type': 'eq', 'fun': lambda x: 1 - np.sum(x)},
                      {'type': 'eq', 'fun': lambda x: mu - np.dot(x, expected_return)})
            w = minimize(MV, x_0, method='SLSQP', constraints=consTR, bounds=bndsa, args=(expected_cov),
                         options={'disp': False})
            volRange[i] = np.dot(w.x, np.dot(expected_cov, w.x.T)) ** 0.5

            wgt[mu].extend(np.squeeze(w.x))

        sharpe = np.array([])

        for i in range(len(muRange)):
            sharpe = np.append(sharpe, muRange[i] / volRange[i])

        self.optimal_weight = wgt[muRange[sharpe.argmax()]]

        for ticker_data in self.portfolio:
            ticker_name = ticker_data["ticker"]
            ticker_pos = ticker_data["position"]
            price = all_indice_df[ticker_name][-1]
            target_pos = self.optimal_weight[all_indice_df.index.get_loc(ticker_name)] * self.total_market_value / price
            pos_change = target_pos - ticker_pos
            if pos_change > 0:
                action_msg = IBActionsTuple(timestamp, IBAction.BUY_MKT_ORDER,
                                            {'ticker': ticker_name, 'position_purchase': pos_change})
                self.action_msgs.append(action_msg)
            elif pos_change < 0:
                action_msg = IBActionsTuple(timestamp, IBAction.SELL_MKT_ORDER,
                                            {'ticker': ticker_name, 'position_sell': pos_change})
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
            if freq == "Daily":
                # next_exec_datetime_obj = self.last_exec_datetime_obj + relativedelta(days=+relative_delta)
                if datetime_obj.day != self.last_exec_datetime_obj.day and datetime_obj > self.last_exec_datetime_obj:
                    self.last_exec_datetime_obj = datetime_obj
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
