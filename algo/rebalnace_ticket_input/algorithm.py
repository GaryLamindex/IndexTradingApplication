"""
class hierarchy suggestion:
abstract base class "algorithm"
And then for any other specify algorithms (e.g., rebalance margin with max drawdown), inhereits the algorithm class and build addtional features
Now put everything together for simplicity, but better separate the base class and the child class
"""
from object.backtest_acc_data import backtest_acc_data
from engine.backtest_engine.trade_engine import backtest_trade_engine
from engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine


class rebalance_wif_ticket_input:
    trade_agent = None
    portfolio_agent = None
    number_of_tickers = 0
    action_msgs = []
    loop = 0
    rebalance_ratio = {}  # for testing
    account_snapshot = {}

    def __init__(self, trade_agent, portfolio_agent, ticker_wif_rebalance_ratio):
        self.ticker_wif_rebalance_ratio = ticker_wif_rebalance_ratio
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.number_of_tickers = len(ticker_wif_rebalance_ratio)
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()

    def run(self, realtime_stock_data_dict):
        self.portfolio_agent.update_stock_price_and_portfolio_data(realtime_stock_data_dict)
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()



def main():
    ticker_wif_rebalance_ratio = {"first": 0.5, "second": 0.5}
    abc = rebalance_wif_ticket_input(None, None, ticker_wif_rebalance_ratio)


if __name__ == "__main__":
    main()
