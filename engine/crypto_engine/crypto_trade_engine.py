class crypto_trade_engine:

    def __init__(self, crypto_acc_data, data_io_engines, portfolio_data_engine):
        self.crypto_acc_data = crypto_acc_data
        self.data_io_engines = data_io_engines
        self.portfolio_data_engine = portfolio_data_engine

    def place_buy_mkt_order(self, ticker, position_purchase, timestamp, price):
        funding = self.crypto_acc_data.wallet.get('funding')
        transaction_amount = position_purchase * price

        if funding < transaction_amount:  # cannot buy
            return None

        # can buy
        self.crypto_acc_data.wallet['funding'] = funding - transaction_amount
        self.crypto_acc_data.update_portfolio_item(ticker, position_purchase, 0)
        return {'timestamp': timestamp, 'ticker': ticker, 'side': 'buy', 'price': price, 'quantity': position_purchase,
                'realized profit': None}

    def place_sell_mkt_order(self, ticker, position_sell, timestamp, price):
        ticker_item = self.crypto_acc_data.check_if_ticker_exist_in_portfolio(ticker)
        if ticker_item:  # sell
            if ticker_item['available'] < position_sell:
                return None
            else:
                funding = self.crypto_acc_data.wallet['funding']
                self.crypto_acc_data.wallet['funding'] = funding + price * position_sell
                self.crypto_acc_data.update_portfolio_item(ticker, ticker_item['available'] - position_sell,
                                                           ticker_item['unavailable'])
                return {'timestamp': timestamp, 'ticker': ticker, 'side': 'sell', 'price': price,
                        'quantity': position_sell}
        else:  # short sell
            pass
