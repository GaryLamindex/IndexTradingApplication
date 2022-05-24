from datetime import datetime

import pandas as pd
from tinydb import TinyDB, where, Query

from engine.backtest_engine.data_calculation_engine import data_calculation_engine


class trade_agent(object):
    path = ""
    db_path = ""
    margin_acc = None
    mkt_value = None
    portfolio = None
    trading_funds = None
    margin_info = None
    cash_record = None
    stock_transaction_record = None
    deposit_withdraw_cash_record = None
    data_calculation_engine = None

    def __init__(self, path):
        self.path = path
        self.db_path = path + "/db"
        self.acc_data = TinyDB(self.db_path + '/acc_data.json')
        self.margin_acc = TinyDB(self.db_path + '/margin_acc.json')
        self.mkt_value = TinyDB(self.db_path + '/mkt_value.json')
        self.portfolio = TinyDB(self.db_path + '/portfolio.json')
        self.trading_funds = TinyDB(self.db_path + '/trading_funds.json')
        self.margin_info = TinyDB(self.db_path + '/margin_info.json')
        self.cash_record = TinyDB(self.db_path+'/cash_record.json')
        self.stock_transaction_record = TinyDB(self.db_path+'/stock_transaction_record.json')
        self.deposit_withdraw_cash_record = TinyDB(self.db_path + "/deposit_withdraw_cash_record.json")
        self.data_calculation_engine = data_calculation_engine(path)

    def place_buy_stock_mkt_order(self, ticker, position_purchase, backtest_data):
        timestamp = backtest_data.get("timestamp")
        ticker_stock_data_path = backtest_data.get("ticker_stock_data_path")

        ticker_stock_data = TinyDB(ticker_stock_data_path)
        ticker_open_price = ticker_stock_data.get(Query().timestamp == timestamp).get("open")
        print("ticker_open_price", ticker_open_price)

        transaction_amount = position_purchase * ticker_open_price
        BuyingPower = self.trading_funds.all()[0].get("BuyingPower")
        TotalCashValue = self.mkt_value.all()[0].get("TotalCashValue")

        print("BuyingPower:", BuyingPower)
        if BuyingPower < transaction_amount:
            print("amount required:",transaction_amount,'; not enough buy_pwr, buy position is rejected')
            ticker = ""
            transaction_amount = 0
            msg = {'ticker': ticker, 'action': 'rejected', 'amount': transaction_amount}
            return msg

        if not (self.portfolio.contains(where('ticker') == ticker)):
            _init_stock = {'ticker': ticker, 'position': 0, "marketPrice": 0, 'averageCost': 0, "marketValue": 0,
                           "realizedPNL": 0, "unrealizedPNL": 0, 'initMarginReq': 0, 'maintMarginReq': 0, "costBasis":0}
            self.portfolio.insert(_init_stock)

        TotalCashValue -= transaction_amount

        # Calculate shares portfolio info
        ticker_info = self.portfolio.get(Query().ticker == ticker)
        costBasis = ticker_info.get("costBasis") + transaction_amount
        position = ticker_info.get("position") + position_purchase
        averageCost = costBasis / position
        marketValue = ticker_open_price * position
        initMarginReq = costBasis * self.margin_info.get(Query().ticker == ticker).get("initMarginReq")
        maintMarginReq = costBasis * self.margin_info.get(Query().ticker == ticker).get("maintMarginReq")

        # Update shares portfolio info
        self.portfolio.update_multiple([({"position": position}, Query().ticker == ticker),
                                        ({"costBasis": costBasis}, Query().ticker == ticker),
                                        ({"averageCost": averageCost}, Query().ticker == ticker),
                                        ({"marketValue": marketValue}, Query().ticker == ticker),
                                        ({"initMarginReq": initMarginReq}, Query().ticker == ticker),
                                        ({"maintMarginReq": maintMarginReq}, Query().ticker == ticker)
                                        ])
        self.mkt_value.update({"TotalCashValue": TotalCashValue}, Query().TotalCashValue.exists())

        # Record stock transaction
        _date = datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
        _time = datetime.fromtimestamp(int(timestamp)).strftime("%H:%M:%S")
        stock_transaction_record_dict = {'ticker': ticker, "timestamp":timestamp,'date': _date, "time":_time, 'transaction_type': 'Buy',
                                         "position_purchase": position_purchase, 'ticker_open_price': ticker_open_price, 'transaction_amount': transaction_amount,
                                         'position': position}
        self.stock_transaction_record.insert(stock_transaction_record_dict)

        print("fully executed")
        print(stock_transaction_record_dict)
        print("")

        # Update Mkt Value & Margin
        self.data_calculation_engine.cal_mkt_value()
        self.data_calculation_engine.cal_margin_info()

        action_msg = {'ticker': ticker, 'action': 'buy', 'transaction_amount': transaction_amount}
        return action_msg

    def place_sell_stock_mkt_order(self, ticker, position_sell, backtest_data):
        mkt_value = TinyDB(self.db_path + '/mkt_value.json')
        portfolio = TinyDB(self.db_path + '/portfolio.json')
        timestamp = backtest_data.get("timestamp")
        ticker_stock_data_path = backtest_data.get("ticker_stock_data_path")

        ticker_stock_data = TinyDB(ticker_stock_data_path)
        ticker_open_price = ticker_stock_data.get(Query().timestamp == timestamp).get("open")
        print("ticker_open_price", ticker_open_price)

        if not (portfolio.contains(Query().ticker == ticker)):
            print('stock not exist, sell action rejected')
            ticker = ""
            transaction_amount = 0
            msg = {'ticker': ticker, 'action': 'none', 'transaction_amount': transaction_amount}
            return msg

        orig_position = portfolio.get(Query().ticker==ticker).get('position')
        if orig_position < position_sell:
            print('shares not enough, sell action rejected')
            ticker = ""
            transaction_amount = 0
            msg = {'ticker': ticker, 'action': 'none', 'transaction_amount': transaction_amount}
            return msg

        transaction_amount = position_sell * ticker_open_price
        TotalCashValue = mkt_value.all()[0].get("TotalCashValue")
        TotalCashValue += transaction_amount

        # Calculate shares portfolio info
        position = portfolio.get(Query().ticker == ticker).get("position") - position_sell
        averageCost = portfolio.get(Query().ticker == ticker).get("averageCost")
        costBasis = portfolio.get(Query().ticker == ticker).get("costBasis") - position_sell * averageCost
        realizedPNL = position * (ticker_open_price - averageCost)
        unrealizedPNL = portfolio.get(Query().ticker == ticker).get("unrealizedPNL") - realizedPNL
        marketValue = ticker_open_price * position

        # Update shares portfolio info
        self.portfolio.update_multiple([({"position": position}, Query().ticker == ticker),
                                   ({"costBasis": costBasis}, Query().ticker == ticker),
                                   ({"realizedPNL": realizedPNL}, Query().ticker == ticker),
                                   ({"marketValue": marketValue}, Query().ticker == ticker),
                                   ({"unrealizedPNL": unrealizedPNL}, Query().ticker == ticker)
                                   ])
        mkt_value.update({"TotalCashValue": TotalCashValue}, Query().TotalCashValue.exists())
        # Update Mkt Value & Margin
        self.data_calculation_engine.cal_mkt_value()
        self.data_calculation_engine.cal_margin_info()

        # Record stock transaction
        _date = datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
        _time = datetime.fromtimestamp(int(timestamp)).strftime("%H:%M:%S")
        stock_transaction_record_dict = {'ticker': ticker, "timestamp":timestamp,'date': _date, "time":_time, 'transaction_type': 'Sell', 'position_sell': position_sell,
                                         'ticker_open_price': ticker_open_price, 'transaction_amount': transaction_amount, 'position': position}
        self.stock_transaction_record.insert(stock_transaction_record_dict)

        print("ticker sold: ", ticker)
        print(stock_transaction_record_dict)
        print("")
        msg = {'ticker': ticker, 'action': 'sell', 'transaction_amount': transaction_amount}
        return msg

