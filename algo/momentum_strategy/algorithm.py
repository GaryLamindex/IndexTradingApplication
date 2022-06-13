class momentum_strategy:
    def __init__(self, trade_agent, portfolio_agent):
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("NetLiquidation")

    def run(self):
        pass

    def calculate_past_n_days_pct_change(self, ticker_dfs, n):
        for df in ticker_dfs:
            df['pct_change'] = df['price'].pct_change(periods=n)
        return ticker_dfs
