class crypto_trade_engine:

    def __init__(self, crypto_acc_data, data_io_engines, portfolio_data_engine):
        self.crypto_acc_data = crypto_acc_data
        self.data_io_engines = data_io_engines
        self.portfolio_data_engine = portfolio_data_engine

    def place_buy_crypto_mkt_order(self, ticker, position_purchase, timestamp, price):
        funding = self.crypto_acc_data.wallet.get('funding')
        transaction_amount = position_purchase * price

        if funding < transaction_amount:  # cannot buy
            return {'timestamp': timestamp, 'ticker': ticker, 'side': 'buy', 'price': price,
                    'quantity': position_purchase,
                    'realized profit': None, 'action': 'rejected'}

        # can buy
        spot = self.crypto_acc_data.wallet['spot']
        rate_btc = self.data_io_engines['BTC'].get_field_by_timestamp(timestamp, 'price')
        self.crypto_acc_data.update_wallet(spot + transaction_amount / rate_btc, funding - transaction_amount)
        self.crypto_acc_data.update_portfolio_item(ticker, position_purchase, 0)
        return {'timestamp': timestamp, 'ticker': ticker, 'side': 'buy', 'price': price, 'quantity': position_purchase,
                'realized profit': None, 'action': 'filled'}

    def place_sell_crypto_mkt_order(self, ticker, position_sell, timestamp, price):
        ticker_item = self.crypto_acc_data.check_if_ticker_exist_in_portfolio(ticker)
        if ticker_item:  # sell
            if ticker_item['available'] < position_sell:
                return {'timestamp': timestamp, 'ticker': ticker, 'side': 'sell', 'price': price,
                        'quantity': position_sell, 'action': 'rejected'}
            else:
                funding = self.crypto_acc_data.wallet['funding']
                rate_btc = self.data_io_engines['BTC'].get_field_by_timestamp(timestamp, 'price')
                transaction_amount = price * position_sell
                spot = self.crypto_acc_data.wallet['spot']
                self.crypto_acc_data.update_wallet(spot - transaction_amount / rate_btc,
                                                   funding + transaction_amount)
                self.crypto_acc_data.update_portfolio_item(ticker, ticker_item['available'] - position_sell,
                                                           ticker_item['unavailable'])

                return {'timestamp': timestamp, 'ticker': ticker, 'side': 'sell', 'price': price,
                        'quantity': position_sell, 'action': 'filled'}
        else:  # short sell
            pass
