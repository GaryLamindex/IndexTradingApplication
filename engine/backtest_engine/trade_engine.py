class backtest_trade_engine(object):
    backtest_acc_data = None
    portfolio_data_engine = None
    stock_data_io_engines = None

    def __init__(self, backtest_acc_data, stock_data_io_engines, portfolio_data_engine):
        self.backtest_acc_data = backtest_acc_data
        self.portfolio_data_engine = portfolio_data_engine
        self.stock_data_io_engines = stock_data_io_engines

    def market_opened(self):
        return True

    def place_buy_stock_limit_order(self, ticker, share_purchase, ticker_price, timestamp):
        meta_data = {"ticker_open_price" : ticker_price, "timestamp":timestamp}
        action_msg = self.place_buy_stock_mkt_order(ticker, share_purchase, meta_data)
        return action_msg

    def place_buy_stock_mkt_order(self, ticker, position_purchase, meta_data):
        trading_funds = self.backtest_acc_data.trading_funds
        mkt_value = self.backtest_acc_data.mkt_value
        margin_acc = self.backtest_acc_data.margin_acc
        deposit_withdraw_cash_record = self.backtest_acc_data.deposit_withdraw_cash_record
        portfolio = self.backtest_acc_data.portfolio
        stock_transaction_record = self.backtest_acc_data.stock_transaction_record

        if meta_data["ticker_open_price"] != None:
            ticker_open_price = meta_data["ticker_open_price"]
            timestamp = meta_data.get("timestamp")
        else:
            timestamp = meta_data.get("timestamp")
            ticker_item = self.stock_data_io_engines[ticker].get_ticker_item_by_timestamp(timestamp)
            ticker_open_price = ticker_item.get("open")
            print("ticker_open_price", ticker_open_price)

        transaction_amount = position_purchase * ticker_open_price
        BuyingPower = trading_funds.get("BuyingPower")
        TotalCashValue = mkt_value.get("TotalCashValue")

        print("BuyingPower:", BuyingPower)
        if BuyingPower < transaction_amount:
            print("amount required:",transaction_amount,'; not enough buy_pwr, buy position is rejected')
            ticker = ""
            transaction_amount = 0
            msg = {'state':'0','ticker': ticker, 'action': 'rejected', 'totalQuantity': position_purchase, 'avgPrice': None}
            return msg

        if self.backtest_acc_data.check_if_ticker_exist_in_portfolio(ticker) == False:
            _init_stock = {'ticker': ticker, 'position': 0, "marketPrice": 0, 'averageCost': 0, "marketValue": 0,
                           "realizedPNL": 0, "unrealizedPNL": 0, 'initMarginReq': 0, 'maintMarginReq': 0, "costBasis":0}
            portfolio.append(_init_stock)

        TotalCashValue -= transaction_amount

        # Calculate shares portfolio info
        portfolio_ticker_item = self.backtest_acc_data.get_portfolio_ticker_item(ticker)
        costBasis = portfolio_ticker_item.get("costBasis") + transaction_amount
        position = portfolio_ticker_item.get("position") + position_purchase
        averageCost = 0
        if position>0:
            averageCost = costBasis / position
        marketValue = ticker_open_price * position
        initMarginReq = costBasis * self.backtest_acc_data.get_margin_info_ticker_item(ticker).get("initMarginReq")
        maintMarginReq = costBasis * self.backtest_acc_data.get_margin_info_ticker_item(ticker).get("maintMarginReq")

        # Update shares portfolio info
        updated_portfolio = {"ticker": ticker, "position": position, "costBasis": costBasis, "averageCost": averageCost, "marketValue": marketValue,
                             "initMarginReq": initMarginReq, "maintMarginReq": maintMarginReq}
        self.backtest_acc_data.update_portfolio_ticker_item(updated_portfolio)
        mkt_value.update({"TotalCashValue": TotalCashValue})

        # Record stock transaction
        transaction_type = 0
        self.backtest_acc_data.append_stock_transaction_record(ticker, timestamp, transaction_type, position_purchase, ticker_open_price, transaction_amount, position)

        print("buy order fully executed")
        print("")

        # Update Mkt Value & Margin
        self.portfolio_data_engine.update_acc_data()

        action_msg = {'state':'1','ticker': ticker, 'action': 'Buy', 'totalQuantity': position_purchase, 'avgPrice':ticker_open_price}
        return action_msg

    def place_sell_stock_mkt_order(self, ticker, position_sell, backtest_data):
        mkt_value = self.backtest_acc_data.mkt_value
        portfolio = self.backtest_acc_data.portfolio
        timestamp = backtest_data.get("timestamp")

        ticker_item = self.stock_data_io_engines[ticker].get_ticker_item_by_timestamp(timestamp)
        ticker_open_price = ticker_item.get("open")
        print("ticker_open_price", ticker_open_price)

        if self.backtest_acc_data.check_if_ticker_exist_in_portfolio(ticker) == False:
            print('stock not exist, sell action rejected')
            ticker = ""
            transaction_amount = 0
            msg = {'state':'0', 'ticker': ticker, 'action': 'none', 'totalQuantity': position_sell, 'avgPrice':ticker_open_price}
            return msg

        portfolio_ticker_item = self.backtest_acc_data.get_portfolio_ticker_item(ticker)
        orig_position = portfolio_ticker_item.get("position")

        if orig_position < position_sell:
            print('shares not enough, sell action rejected')
            ticker = ""
            transaction_amount = 0
            msg = {'state':'0', 'ticker': ticker, 'action': 'none', 'totalQuantity': position_sell, 'avgPrice':ticker_open_price}
            return msg

        transaction_amount = position_sell * ticker_open_price
        TotalCashValue = mkt_value.get("TotalCashValue")
        TotalCashValue += transaction_amount

        # Calculate shares portfolio info
        ticker_item = self.backtest_acc_data.get_portfolio_ticker_item(ticker)
        position = ticker_item.get("position") - position_sell
        averageCost = ticker_item.get("averageCost")
        costBasis = ticker_item.get("costBasis") - position_sell * averageCost
        realizedPNL = position * (ticker_open_price - averageCost)
        unrealizedPNL = ticker_item.get("unrealizedPNL") - realizedPNL
        marketValue = ticker_open_price * position
        initMarginReq = costBasis * self.backtest_acc_data.get_margin_info_ticker_item(ticker).get("initMarginReq")
        maintMarginReq = costBasis * self.backtest_acc_data.get_margin_info_ticker_item(ticker).get("maintMarginReq")

        # Update shares portfolio info
        updated_portfolio = {"ticker": ticker, "position": position, "costBasis": costBasis, "averageCost": averageCost,
                             "marketValue": marketValue,"realizedPNL": realizedPNL, "unrealizedPNL": unrealizedPNL, "marketValue": marketValue,
                             "initMarginReq": initMarginReq, "maintMarginReq": maintMarginReq}
        self.backtest_acc_data.update_portfolio_ticker_item(updated_portfolio)
        mkt_value.update({"TotalCashValue": TotalCashValue})

        # Update Mkt Value & Margin
        self.portfolio_data_engine.update_acc_data()

        # Record stock transaction
        transaction_type = 1
        self.backtest_acc_data.append_stock_transaction_record(ticker, timestamp, transaction_type, position_sell, ticker_open_price, transaction_amount, position)


        print("ticker sold: ", ticker)
        msg = {'state':'1','ticker': ticker, 'action': 'sell', 'totalQuantity': position_sell, 'avgPrice':ticker_open_price}
        return msg

    def place_sell_stock_limit_order(self, ticker, share_sold, ticker_price, timestamp):
        meta_data = {"ticker_open_price" : ticker_price, "timestamp":timestamp}
        action_msg = self.place_sell_stock_mkt_order(ticker, share_sold, meta_data)
        return action_msg