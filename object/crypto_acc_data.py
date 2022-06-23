class crypto_acc_data():
    user_id = 0

    def __init__(self, user_id, strategy_name, table_name, spec_str):
        self.user_id = user_id
        self.strategy_name = strategy_name
        self.table_name = table_name
        self.stock_transaction_record = []
        self.portfolio = []
        self.cashflow_record = []
        # spot is currently unavailable for backtest
        # it requires coin to BTC exchange rate
        self.wallet = {'spot': 0, 'funding': 0}

    def update_portfolio_item(self, ticker, available, unavailable):
        temp_dict = {'ticker': ticker, 'available': available, 'unavailable': unavailable,
                     'total': available + unavailable}
        self.portfolio.append(temp_dict)

    def update_wallet(self, spot, funding):
        self.wallet.update({'spot': spot, 'funding': funding})

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
