import os
from os import listdir
from time import sleep
from pathlib import Path

from ib.opt import ibConnection
from ib.opt import ibConnection, message
from tinydb import TinyDB, Query


class ibkr_portfolio_data_agent(object):
    accountName = "U6789998"
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

    latest_time_stamp = ""
    _position_data = {}
    _position_data_arr = []
    _margin_acc_data = {}
    _mkt_value_data = {}
    _portfolio_data = {}
    _trading_funds_data = {}
    _margin_info_data = {}
    _cash_record_data = {}
    _stock_transaction_record_data = {}
    _deposit_withdraw_cash_record_data = {}
    _acc_data = {}

    def __init__(self, path):
        pass

    def update_acc_portfolio_from_tws(self):

        def acct_data_handler(msg):
            if hasattr(msg, 'contract'):
                c = msg.contract
                ticker = '%s' % (c.m_symbol)
                sec_type = '%s' % (c.m_secType)
                exchange = '%s' % (c.m_exchange)
                position = msg.position
                marketPrice = msg.marketPrice
                marketValue = msg.marketValue
                averageCost = msg.averageCost
                unrealizedPNL = msg.unrealizedPNL
                realizedPNL = msg.realizedPNL
                costBasis = averageCost*position
                _position_data = {"ticker":ticker, "position":position, "marketPrice":marketPrice,"marketValue":marketValue,
                                  "averageCost":averageCost, "unrealizedPNL":unrealizedPNL,"realizedPNL":realizedPNL, "costBasis":costBasis}
                self._position_data_arr.append(_position_data)

            if hasattr(msg, 'timeStamp'):
                self.latest_time_stamp = msg.timeStamp

            if hasattr(msg, 'key'):
                if msg.key == "Leverage-S":
                    self._margin_acc_data.update({"Leverage":msg.value})
                if hasattr(msg, 'currency'):
                    if msg.currency == "HKD":
                        if msg.key == "AccountCode":
                            self._acc_data.update({msg.key:msg.value})
                        elif msg.key == "AvailableFunds":
                            self._trading_funds_data.update({msg.key:msg.value})
                        elif msg.key == "BuyingPower":
                            self._trading_funds_data.update({msg.key:msg.value})
                        elif msg.key == "Currency":
                            self._acc_data.update({msg.key:msg.value})
                        elif msg.key == "EquityWithLoanValue":
                            self._trading_funds_data.update({msg.key:msg.value})
                        elif msg.key == "ExcessLiquidity":
                            self._trading_funds_data.update({msg.key:msg.value})
                        elif msg.key == "ExchangeRate":
                            self._acc_data.update({msg.key:msg.value})
                        elif msg.key == "FullInitMarginReq":
                            self._margin_acc_data.update({msg.key:msg.value})
                        elif msg.key == "FullMaintMarginReq":
                            self._margin_acc_data.update({msg.key:msg.value})
                        elif msg.key == "GrossPositionValue":
                            self._mkt_value_data.update({msg.key:msg.value})
                        elif msg.key == "NetDividend":
                            self._mkt_value_data.update({msg.key:msg.value})
                        elif msg.key == "NetLiquidation":
                            self._mkt_value_data.update({msg.key:msg.value})
                        elif msg.key == "TotalCashValue":
                            self._mkt_value_data.update({msg.key:msg.value})

                    elif msg.currency == "BASE":
                        if msg.key == "RealizedPnL":
                            self._mkt_value_data.update({msg.key:msg.value})
                        elif msg.key == "UnrealizedPnL":
                            self._mkt_value_data.update({msg.key:msg.value})
                        elif msg.key == "StockMarketValue":
                            self._mkt_value_data.update({msg.key:msg.value})


        con = ibConnection()
        con.register(acct_data_handler,
                     message.updateAccountValue,
                     message.updateAccountTime,
                     message.updatePortfolio)
        con.connect()
        con.reqAccountUpdates(True, self.accountName)
        sleep(1)

        for _data in self._position_data_arr:
            _data.update({"timeStamp": self.latest_time_stamp})
        self._acc_data.update({"timeStamp": self.latest_time_stamp})
        self._margin_acc_data.update({"timeStamp": self.latest_time_stamp})
        self._mkt_value_data.update({"timeStamp": self.latest_time_stamp})
        self._trading_funds_data.update({"timeStamp": self.latest_time_stamp})

        print(self._margin_acc_data)
        print(self._mkt_value_data)
        print(self._trading_funds_data)

        self.margin_acc.truncate()
        self.mkt_value.truncate()
        self.trading_funds.truncate()
        self.portfolio.truncate()

        self.margin_acc.insert(self._margin_acc_data)
        self.mkt_value.insert(self._mkt_value_data)
        self.trading_funds.insert(self._trading_funds_data)

        for _data in self._position_data_arr:
            self.portfolio.insert(_data)

        # don't forget to disconnect somehow when done
        con.disconnect()

    def get_acct_snapshot(self):
        self.update_acc_portfolio_from_tws()
        snapshot_dict = {}
        if len(self.portfolio.all()) == 0:
            snapshot_dict = {
                "TotalCashValue": self.mkt_value.all()[0].get("TotalCashValue"),
                "GrossPositionValue": 0,
                "ExcessLiquidity": self.trading_funds.all()[0].get("ExcessLiquidity"),
                "BuyingPower": self.trading_funds.all()[0].get("BuyingPower"),
                "AvailableFunds": self.trading_funds.all()[0].get("AvailableFunds"),
                "FullInitMarginReq": 0,
                "Leverage": self.trading_funds.all()[0].get("Leverage"),
                "NetLiquidation": self.mkt_value.all()[0].get("NetLiquidation"),
            }
        else:
            snapshot_dict.update({
                "TotalCashValue": self.mkt_value.all()[0].get("TotalCashValue"),
                "GrossPositionValue": self.mkt_value.all()[0].get("GrossPositionValue"),
                "ExcessLiquidity": self.trading_funds.all()[0].get("ExcessLiquidity"),
                "BuyingPower": self.trading_funds.all()[0].get("BuyingPower"),
                "AvailableFunds": self.trading_funds.all()[0].get("AvailableFunds"),
                "FullInitMarginReq": self.margin_acc.all()[0].get("FullInitMarginReq"),
                "Leverage": self.trading_funds.all()[0].get("Leverage"),
                "NetLiquidation": self.mkt_value.all()[0].get("NetLiquidation"),
            })
            tickers = [r['ticker'] for r in self.portfolio.all()]
            for ticker in tickers:
                snapshot_dict.update({
                    ticker + "_position": self.portfolio.get(Query().ticker == ticker).get("position"),
                    ticker + "_marketPrice": self.portfolio.get(Query().ticker == ticker).get("marketPrice"),
                    ticker + "_marketValue": self.portfolio.get(Query().ticker == ticker).get("marketValue"),
                    ticker + "_averageCost": self.portfolio.get(Query().ticker == ticker).get("averageCost")
                })
        return snapshot_dict

    def init_data(self):
        os.makedirs(self.path+"/db", exist_ok=True)
        os.makedirs(self.path + "/sim_data/csv", exist_ok=True)
        os.makedirs(self.path + "/sim_data/json", exist_ok=True)
        os.makedirs(self.path + "/stats_data/csv", exist_ok=True)
        os.makedirs(self.path + "/stats_data/json", exist_ok=True)

        list_of_db = listdir(self.db_path)
        list_of_sim_data_csv = listdir(self.path + "/sim_data/csv")
        list_of_sim_data_json = listdir(self.path + "/sim_data/json")
        list_of_stats_data_csv = listdir(self.path + "/stats_data/csv")
        list_of_stats_data_json = listdir(self.path + "/stats_data/json")

        for file in list_of_db:
            os.remove(Path(self.db_path+"/csv/" + file))
        for file in list_of_sim_data_csv:
            os.remove(Path(self.path + "/sim_data/csv"+ file))
        for file in list_of_sim_data_json:
            os.remove(Path(self.path + "/sim_data/json"+ file))
        for file in list_of_stats_data_csv:
            os.remove(Path(self.path + "/stats_data/csv"+ file))
        for file in list_of_stats_data_json:
            os.remove(Path(self.path + "/stats_data/json"+ file))

        pass


    def init_portfolio_db(self):

        self.acc_data = TinyDB(self.db_path + '/acc_data.json')
        self.margin_acc = TinyDB(self.db_path + '/margin_acc.json')
        self.mkt_value = TinyDB(self.db_path + '/mkt_value.json')
        self.portfolio = TinyDB(self.db_path + '/portfolio.json')
        self.trading_funds = TinyDB(self.db_path + '/trading_funds.json')
        self.margin_info = TinyDB(self.db_path + '/margin_info.json')

        self.margin_acc.truncate()
        self.mkt_value.truncate()
        self.portfolio.truncate()
        self.trading_funds.truncate()
        self.margin_info.truncate()
        self.cash_record.truncate()
        self.stock_transaction_record.truncate()
        self.deposit_withdraw_cash_record.truncate()

        self.acc_data.insert({"AccountCode": 0, "Currency": "HKD", "ExchangeRate": 0})
        self.margin_acc.insert({'FullInitMarginReq': 0, 'FullMaintMarginReq': 0})
        self.trading_funds.insert({'AvailableFunds': 0, "ExcessLiquidity": 0, "BuyingPower": 0, "Leverage": 0,
                                   "EquityWithLoanValue": 0})
        self.mkt_value.insert(
            {"TotalCashValue": 0, "NetDividend": 0, "NetLiquidation": 0, "UnrealizedPnL": 0, "RealizedPnL": 0,
             "GrossPositionValue": 0})
        _spy = {'ticker': 'SPY', 'initMarginReq': 0.09, 'maintMarginReq': 0.1}
        _qqq = {'ticker': 'QQQ', 'initMarginReq': 0.1, 'maintMarginReq': 0.11}
        _govt = {'ticker': 'GOVT', 'initMarginReq': 0.09, 'maintMarginReq': 0.1}
        _shv = {'ticker': 'SHV', 'initMarginReq': 0.09, 'maintMarginReq': 0.1}
        self.margin_info.insert_multiple([_spy, _qqq, _govt, _shv])
