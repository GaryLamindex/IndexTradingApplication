"""
class hierarchy suggestion:
abstract base class "algorithm"
And then for any other specify algorithms (e.g., rebalance margin with max drawdown), inhereits the algorithm class and build addtional features
Now put everything together for simplicity, but better separate the base class and the child class
"""
from object.backtest_acc_data import backtest_acc_data
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
from dateutil.relativedelta import relativedelta



class portfolio_rebalance:
    trade_agent = None
    portfolio_agent = None
    action_msgs = []
    loop = 0
    acceptance_range = 0
    ticker_wif_rebalance_ratio = {}
    account_snapshot = {}
    total_market_value = 0
    portfolio = {}
    target_market_positions = {}
    buy_list = []
    sell_list = []

    def __init__(self, trade_agent, portfolio_agent, ticker_wif_rebalance_ratio, acceptance_range):
        self.ticker_wif_rebalance_ratio = ticker_wif_rebalance_ratio
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.acceptance_range = acceptance_range
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("mkt_value").get("NetLiquidation")

    def run(self, realtime_stock_data_dict, timestamp):

        self.action_msgs = []

        if not self.trade_agent.market_opened():
            return

        self.portfolio_agent.update_stock_price_and_portfolio_data(realtime_stock_data_dict)
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("mkt_value").get("NetLiquidation")
        for ticker, percentage in self.ticker_wif_rebalance_ratio:
            current_market_price = self.portfolio.get("marketPrice")
            target_position = int((self.total_market_value * percentage) / current_market_price)
            self.target_market_positions.update({ticker: target_position})
        unmodified_tickers = list(self.target_market_positions.keys())
        for ticker_info in self.portfolio:
            unmodified_flag = True
            current_position = ticker_info.get("position")
            for target_ticker, target_pos in self.target_market_positions:
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
            self.buy_list.append([ticker, self.target_market_positions.get("ticker")])

        for ticker in self.sell_list:
            action_msg = self.trade_agent.place_sell_stock_mkt_order(ticker[0], ticker[1], timestamp)
            self.action_msgs.append(action_msg)
        for ticker in self.buy_list:
            action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker[0], ticker[1], timestamp)
            self.action_msgs.append(action_msg)

        return self.action_msgs.copy()


def main():
    pass


if __name__ == "__main__":
    main()
