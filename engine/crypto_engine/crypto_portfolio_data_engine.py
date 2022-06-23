class crypto_portfolio_data_engine:
    def __init__(self, acc_data, tickers):
        self.acc_data = acc_data
        self.tickers = tickers

    def deposit_cash(self, account, amount, timestamp):
        if account == 'cross_margin':
            pass
        elif account == 'isolated_margin':
            pass
        elif account == 'funding':
            temp = self.acc_data.wallet['funding']
            self.acc_data.wallet['funding'] = temp + amount
            self.acc_data.append_cashflow_record(timestamp, 2, 0, amount)

    def withdraw(self, account, amount, timestamp):
        if account == 'cross_margin':
            pass
        elif account == 'isolated_margin':
            pass
        elif account == 'funding':
            temp = self.acc_data.wallet['funding']
            self.acc_data.wallet['funding'] = temp - amount
            self.acc_data.append_cashflow_record(timestamp, 2, 1, amount)

    def get_overview(self):
        overview = {}
        overview.update()
