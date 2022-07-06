class crypto_acc_data():
    user_id = 0

    def __init__(self, user_id, strategy_name, table_name, spec_str):
        self.user_id = user_id
        self.strategy_name = strategy_name
        self.table_name = table_name
        self.stock_transaction_record = []
        self.portfolio = []
        self.cashflow_record = []
        # spot might not be accurate
        # currently transfer the coin to USDT and to BTC
        self.wallet = {'spot': 0, 'funding': 0, 'NetLiquidation': 0}

    def remove_portfolio_item(self, ticker):
        self.portfolio = [p for p in self.portfolio if p['ticker'] != ticker]

    def clear_portfolio_item(self):
        self.portfolio.clear()

    def update_portfolio_item(self, ticker, available, unavailable):
        total = available + unavailable
        if total == 0:
            self.remove_portfolio_item(ticker)
            return
        temp_dict = {'ticker': ticker, 'available': available, 'unavailable': unavailable,
                     'total': total}
        for p in self.portfolio:
            if p['ticker'] == ticker:
                p.update(temp_dict)
                return
        self.portfolio.append(temp_dict)

    def update_wallet(self, spot, funding, NetLiquidation):
        self.wallet.update({'spot': spot, 'funding': funding, 'NetLiquidation': NetLiquidation})

    def append_cashflow_record(self, timestamp, account_type, transaction_type, amount):
        transaction_type_dict = {0: "Deposit", 1: "Withdraw"}
        account_type_dict = {0: 'cross_margin', 1: 'isolated_margin',
                             2: 'funding'}

        record = {'timestamp': timestamp, 'account': account_type_dict.get(account_type),
                  transaction_type: transaction_type_dict.get(transaction_type),
                  'amount': amount}
        self.cashflow_record.append(record)

    def append_stock_transaction_record(self, ticker, timestamp, transaction_type, position_purchase, ticker_open_price,
                                        transaction_amount,
                                        position):
        transaction_type_dict = {0: "Buy", 1: "Sell"}
        record = {'ticker': ticker, "timestamp": timestamp,
                  'transaction_type': transaction_type_dict.get(transaction_type),
                  "position_purchase": position_purchase, 'ticker_open_price': ticker_open_price,
                  'transaction_amount': transaction_amount,
                  'position': position}
        self.stock_transaction_record.append(record)

    def check_if_ticker_exist_in_portfolio(self, ticker):
        for i in self.portfolio:
            if i['ticker'] == ticker:
                return i
        return None
