import os
from os import listdir
from pathlib import Path

import pandas as pd
from tinydb import TinyDB, Query


class portfolio_data_agent(object):

    db_path = ""
    margin_acc = None
    mkt_value = None
    portfolio = None
    trading_funds = None
    margin_info = None
    cash_record = None
    stock_transaction_record = None
    deposit_withdraw_cash_record = None

    def __init__(self, path):

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


    def get_acct_snapshot(self):
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


    def init_portfolio_db(self):

        self.acc_data = TinyDB(self.db_path + '/acc_data.json')
        self.margin_acc = TinyDB(self.db_path + '/margin_acc.json')
        self.mkt_value = TinyDB(self.db_path + '/mkt_value.json')
        self.portfolio = TinyDB(self.db_path + '/portfolio.json')
        self.trading_funds = TinyDB(self.db_path + '/trading_funds.json')
        self.margin_info = TinyDB(self.db_path + '/margin_info.json')

        self.acc_data.truncate()
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
