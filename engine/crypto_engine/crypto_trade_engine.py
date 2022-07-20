from object.action_data import BinanceActionMessage, BinanceAction, BinanceActionState


class crypto_trade_engine:

    def __init__(self, crypto_acc_data, data_io_engines, portfolio_data_engine):
        self.crypto_acc_data = crypto_acc_data
        self.data_io_engines = data_io_engines
        self.portfolio_data_engine = portfolio_data_engine

    def place_buy_crypto_mkt_order(self, ticker, position_purchase, timestamp, price):
        old_funding = self.crypto_acc_data.wallet.get('funding')
        transaction_amount = position_purchase * price

        if old_funding < transaction_amount:  # cannot buy
            return BinanceActionMessage(timestamp, ticker, BinanceAction.BUY_MKT_ORDER,
                                        position_purchase, price, 0, BinanceActionState.REJECTED)

        # can buy
        ticker_item = self.crypto_acc_data.check_if_ticker_exist_in_portfolio(ticker)
        if ticker_item:
            self.crypto_acc_data.update_portfolio_item(ticker, ticker_item['available'] + position_purchase,
                                                       ticker_item['unavailable'])
        else:
            self.crypto_acc_data.update_portfolio_item(ticker, position_purchase, 0)
        rate_btc = self.data_io_engines['BTC'].get_field_by_timestamp(timestamp, 'Open')

        new_spot = 0
        for p in self.crypto_acc_data.portfolio:
            ticker = p['ticker']
            rate = self.data_io_engines[ticker].get_field_by_timestamp(timestamp, 'Open')
            new_spot += rate * p['available'] / self.data_io_engines['BTC'].get_field_by_timestamp(timestamp, 'Open')
        new_funding = old_funding - transaction_amount
        new_net_liquidation = new_spot * rate_btc + new_funding
        self.crypto_acc_data.update_wallet(new_spot, new_funding, new_net_liquidation)
        return BinanceActionMessage(timestamp, ticker, BinanceAction.BUY_MKT_ORDER,
                                    position_purchase, price, None, BinanceActionState.FILLED)

    def place_sell_crypto_mkt_order(self, ticker, position_sell, timestamp, price):
        ticker_item = self.crypto_acc_data.check_if_ticker_exist_in_portfolio(ticker)
        if ticker_item:  # sell
            if ticker_item['available'] < position_sell:  # not enough to sell
                return {'timestamp': timestamp, 'ticker': ticker, 'side': 'sell', 'price': price,
                        'quantity': position_sell, 'action': 'rejected'}
            else:  # enough to sell
                self.crypto_acc_data.update_portfolio_item(ticker, ticker_item['available'] - position_sell,
                                                           ticker_item['unavailable'])

                old_funding = self.crypto_acc_data.wallet['funding']
                rate_btc = self.data_io_engines['BTC'].get_field_by_timestamp(timestamp, 'Open')
                transaction_amount = price * position_sell

                new_spot = 0
                for p in self.crypto_acc_data.portfolio:
                    ticker = p['ticker']
                    rate = self.data_io_engines[ticker].get_field_by_timestamp(timestamp, 'Open')
                    new_spot += rate * p['available'] / self.data_io_engines['BTC'].get_field_by_timestamp(timestamp,
                                                                                                           'Open')
                new_funding = old_funding + transaction_amount
                new_net_liquidation = new_spot * rate_btc + new_funding
                self.crypto_acc_data.update_wallet(new_spot, new_funding, new_net_liquidation)

                return BinanceActionMessage(timestamp, ticker, BinanceAction.SELL_MKT_ORDER,
                                            position_sell, price, None, BinanceActionState.FILLED)
                # return {'timestamp': timestamp, 'ticker': ticker, 'side': 'sell', 'price': price,
                #         'quantity': position_sell, 'action': 'filled'}
        else:  # short sell
            pass
