import pandas as pd


class momentum_strategy:
    def __init__(self, trade_agent, portfolio_agent):
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("NetLiquidation")

    def run(self, stock_price_dict, periods_pct_change_dict, timestamp):
        df = pd.DataFrame.from_dict(stock_price_dict, orient='index', columns=['price'])
        df
