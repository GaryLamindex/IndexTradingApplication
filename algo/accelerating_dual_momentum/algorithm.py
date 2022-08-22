from object.backtest_acc_data import backtest_acc_data
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
from object.action_data import IBAction, IBActionsTuple


class accelerating_dual_momentum:

    def __init__(self, trade_agent, portfolio_agent):
        self.account_snapshot = {}
        self.portfolio = []
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.pct_change_dict = {}
        self.last_exec_datetime_obj = None
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("NetLiquidation")
        self.action_msgs = []

    def run(self, pct_change_dict, price_dict, bond, timestamp):
        if self.check_exec(timestamp, freq="Monthly", relative_delta=1):
            self.action_msgs = []
            self.pct_change_dict = pct_change_dict
            if not self.trade_agent.market_opened():
                return
            self.portfolio_agent.update_stock_price_and_portfolio_data(price_dict)
            self.account_snapshot = self.portfolio_agent.get_account_snapshot()
            self.portfolio = self.portfolio_agent.get_portfolio()
            self.total_market_value = self.account_snapshot.get("NetLiquidation")
            ticker_list = [*pct_change_dict]
            momentum_signals = []
            buy = ""
            for ticker in ticker_list:
                one_month_pct_change = pct_change_dict[ticker][1]
                three_month_pct_change = pct_change_dict[ticker][3]
                six_month_pct_change = pct_change_dict[ticker][6]
                momentum_signal = one_month_pct_change * 0.33 + three_month_pct_change * 0.33 + six_month_pct_change * 0.34
                momentum_signals.append(momentum_signal)
            if momentum_signals[0] < 0 and momentum_signals[1] < 0:
                buy = bond
            elif momentum_signals[0] > momentum_signals[1]:
                buy = ticker_list[0]
            elif momentum_signals[0] < momentum_signals[1]:
                buy = ticker_list[1]
            for ticker_data in self.portfolio:
                ticker_name = ticker_data["ticker"]
                ticker_pos = ticker_data["position"]
                if ticker_name == buy:
                    if ticker_pos > 0:  # if holding a ticker that need to buy
                        price = price_dict[buy]["last"]
                        target_pos = self.total_market_value / price
                        buy_pos = target_pos - ticker_pos
                        if buy_pos > 0.0:
                            action_msg = IBActionsTuple(timestamp, IBAction.BUY_MKT_ORDER,
                                                        {'ticker': buy, 'position_purchase': buy_pos})
                            self.action_msgs.append(action_msg)
                    elif ticker_pos == 0:  # if not holding a ticker that need to buy
                        price = price_dict[buy]["last"]
                        target_pos = self.total_market_value / price
                        action_msg = IBActionsTuple(timestamp, IBAction.BUY_MKT_ORDER,
                                                    {'ticker': buy, 'position_purchase': target_pos})
                        self.action_msgs.append(action_msg)
                elif ticker_pos > 0:  # if holding a ticker that need to sell
                    sell = ticker_name
                    sell_pos = ticker_pos
                    action_msg = IBActionsTuple(timestamp, IBAction.SELL_MKT_ORDER,
                                                {'ticker': sell, 'position_sell': sell_pos})
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
