class crypto_portfolio_data_engine:
    def __init__(self, acc_data, tickers):
        self.acc_data = acc_data
        self.tickers = tickers

    def deposit_cash(self, amount, timestamp):
