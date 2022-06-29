import certifi
from pymongo import MongoClient
from bson.json_util import dumps
from flask import Flask, app
#from flask_restful import Api, Resource

class Api_Mongodb_3:
    def __init__(self):
        try:
            self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=certifi.where())
            print("Successful connection to mongoClient")
        except:
            print("WARNING: Could not connect to MongoDB")

    def all_algo_3i(self, strategy_name='backtest_rebalance_margin_wif_max_drawdown_control_0'):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor1 = col.find({"strategy_name":strategy_name}, {"_id":0 ,"Rating": 1})
        list_cur1 = list(cursor1)
        json_data = dumps(list_cur1)
        print(json_data)
        return json_data


    def all_algo_3j(self, strategy_name='backtest_rebalance_margin_wif_max_drawdown_control_0'):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor1 = col.find({'strategy_name':strategy_name}, {"_id": 0,"Since Inception Return": 1, "1 Yr Sharpe": 1,
                                                             "5 Yr Sharpe": 1, "YTD Sharpe": 1, "1 Yr Return":1,
                                                             "5 Yr Return":1, "YTD Return":1,
                                                             "Since Inception Sortino": 1, "Since Inception Max Drawdown":1,
                                                             "Since Inception Volatility":1,"Since Inception Win Rate": 1,
                                                             "Since Inception Average Win Per Day":1, "Profit Loss Ratio":1,
                                                             "last nlv":1,
                                                             "Margin Ratio": 1})
        list_cur1 = list(cursor1)
        json_data = dumps(list_cur1)
        print(json_data)
        return json_data

    def all_algo_3k(self, strategy_name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Transactions
        cursor = col.find({"strategy_name":strategy_name}, {"_id":0, "ETF_name":1, "date_time":1, "price":1, "quantity":1, "proceeds":1})

        json_data = dumps(cursor)
        print(json_data)
        return json_data


def main():
    a = Api_Mongodb_3()
    # a.all_algo_1a()
    # a.all_algo_1b()
    # a.all_algo_1c("")
    # a.user_acc_2a()
    # a.user_acc_2b()
    # a.user_acc_2c()
    # a.user_acc_2d()
    # a.user_acc_2e()
    # a.user_acc_2f()
    a.all_algo_3i()
    # a.all_algo_3j()
    # a.all_algo_3k()

if __name__ == "__main__":
        main()