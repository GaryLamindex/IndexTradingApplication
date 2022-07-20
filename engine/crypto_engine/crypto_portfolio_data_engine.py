class crypto_portfolio_data_engine:
    def __init__(self, acc_data, tickers):
        self.acc_data = acc_data
        self.tickers = tickers

    # add cash to the porfolio
    # ONLY cash account is available
    def deposit_cash(self, account, amount, timestamp):
        if account == 'cross_margin':  # to be designed
            pass
        elif account == 'isolated_margin':  # to be designed
            pass
        elif account == 'funding':
            temp = self.acc_data.wallet['funding']
            self.acc_data.wallet['funding'] = temp + amount
            self.acc_data.append_cashflow_record(timestamp, 2, 0, amount)

    # withdraw cash
    # ONLY cash account is available
    def withdraw(self, account, amount, timestamp):
        if account == 'cross_margin':  # to be designed
            pass
        elif account == 'isolated_margin':  # to be designed
            pass
        elif account == 'funding':
            temp = self.acc_data.wallet['funding']
            self.acc_data.wallet['funding'] = temp - amount
            self.acc_data.append_cashflow_record(timestamp, 2, 1, amount)

    # get a overview of the portfolio
    def get_overview(self):
        overview = {}
        overview.update(self.acc_data.wallet)
        for p in self.acc_data.portfolio:
            temp_list = p.copy()
            ticker = temp_list['ticker']
            del temp_list['ticker']
            res = {f'{str(key)}_{ticker}': val for key, val in temp_list.items()}
            overview.update(res)
        return overview
