"""
class hierarchy suggestion:
abstract base class "algorithm"
And then for any other specify algorithms (e.g., rebalance margin with max drawdown), inhereits the algorithm class and build addtional features
Now put everything together for simplicity, but better separate the base class and the child class
"""
from object.backtest_acc_data import backtest_acc_data
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
from object.action_data import IBAction, IBActionsTuple


class portfolio_rebalance:
    trade_agent = None
    portfolio_agent = None
    action_msgs = []
    loop = 0
    acceptance_range = 0
    ticker_wif_rebalance_ratio = {}
    account_snapshot = {}
    net_liquidation = 0
    portfolio = []
    target_market_positions = {}
    buy_list = []
    sell_list = []
    last_exec_datetime_obj = None

    def __init__(self, trade_agent, portfolio_agent, ticker_wif_rebalance_ratio, acceptance_range):
        self.ticker_wif_rebalance_ratio = ticker_wif_rebalance_ratio
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.acceptance_range = acceptance_range
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("NetLiquidation")

    def run(self, realtime_stock_data_dict, timestamp):

        self.action_msgs = []
        self.buy_list = []
        self.sell_list = []

        if not self.trade_agent.market_opened():
            return

        self.portfolio_agent.update_stock_price_and_portfolio_data(realtime_stock_data_dict)
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.portfolio = self.portfolio_agent.get_portfolio()
        self.net_liquidation = self.account_snapshot.get("NetLiquidation")
        for ticker, percentage in self.ticker_wif_rebalance_ratio.items():
            hold_flag = False
            for ticker_info in self.portfolio:
                if ticker_info.get("ticker") == ticker:
                    hold_flag = True
                    current_market_price = ticker_info.get("marketPrice")
                    target_position = int((self.net_liquidation * (percentage / 100)) / current_market_price)
                    self.target_market_positions.update({ticker: target_position})
            if (hold_flag == False):
                for ticker_name, market_price in realtime_stock_data_dict.items():
                    if ticker_name == ticker:
                        current_market_price = market_price.get('last')
                        target_position = int((self.net_liquidation * (percentage / 100)) / current_market_price)
                        self.target_market_positions.update({ticker: target_position})
        unmodified_tickers = list(self.target_market_positions.keys())
        for ticker_info in self.portfolio:
            unmodified_flag = True
            current_position = ticker_info.get("position")
            for target_ticker, target_pos in self.target_market_positions.items():
                if ticker_info.get("ticker") == target_ticker:
                    if current_position < target_pos:
                        self.buy_list.append([target_ticker, target_pos - current_position])
                    elif current_position > target_pos:
                        self.sell_list.append([target_ticker, current_position - target_pos])
                    unmodified_flag = False
                    unmodified_tickers.remove(target_ticker)
                    break
                else:
                    continue
            if unmodified_flag:
                self.sell_list.append([ticker_info.get("ticker"), current_position])
        for ticker in unmodified_tickers:
            self.buy_list.append([ticker, self.target_market_positions.get(ticker)])
        #realtime_stock_data_dict["timestamp"] = timestamp
        for ticker in self.sell_list:
            action_msg = IBActionsTuple(timestamp, IBAction.SELL_MKT_ORDER,
                                      {'ticker': ticker[0], 'position_sell': ticker[1]})
            # action_msg = self.trade_agent.place_sell_stock_mkt_order(ticker[0], ticker[1], realtime_stock_data_dict )
            self.action_msgs.append(action_msg)
        for ticker in self.buy_list:
            action_msg = IBActionsTuple(timestamp, IBAction.BUY_MKT_ORDER,
                                      {'ticker': ticker[0], 'position_purchase': ticker[1]})
            # action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker[0], ticker[1], realtime_stock_data_dict )
            self.action_msgs.append(action_msg)

        return self.action_msgs.copy()

    def check_exec(self, timestamp, **kwargs):
        datetime_obj = datetime.utcfromtimestamp(timestamp)
        if self.last_exec_datetime_obj == None:
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


if __name__ == "__main__":
    pass
    # sim_account = {"portfolio": [{'ticker': "3188", 'position': 2624, "marketPrice": 70},
    #                              {'ticker': "QQQ", 'position': 300, "marketPrice": 50},
    #                              {'ticker': "SPY", 'position': 2274, "marketPrice": 100}
    #                              ],
    #                "mkt_value": {"NetLiquidation": 419830}}
    # rebalance_ratio = {"3188": 50, "SPY": 35,"AAPL": 15}
    # ticker_not_own = [{'ticker': "AAPL", "marketPrice": 20}]
    # sim = portfolio_rebalance(None, None, rebalance_ratio, 0, sim_account)
    #
    # sim.run(ticker_not_own, None)
    # print("NetLiquidation:", sim.net_liquidation)
    # print("buy list:", sim.buy_list)
    # print("sell list", sim.sell_list)
    # print("final_portfolio", sim.target_market_positions)
    # print("action msg",sim.action_msgs)
