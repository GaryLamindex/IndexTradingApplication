import certifi
from pymongo import MongoClient
from bson.json_util import dumps
from flask import Flask, app
from flask_restful import Api, Resource

class Api_Mongodb:
    def __init__(self):
        try:
            self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=certifi.where())
            print("Successful connection to mongoClient")
        except:
            print("WARNING: Could not connect to MongoDB")

    # @app.route('/mainpage')
    def all_algo_1a(self):
        self.db = self.conn["simulation"]
        col = self.db.backtest_portfolio_rebalance_0
        cursor = col.find({}, {"_id": 0, "NetLiquidation": 1, "timestamp": 1}).sort("_id", 1)
        # list_cur = list(cursor)
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    def all_algo_1b(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor1 = col.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1, "YTD Return": 1, "5 Yr Return": 1, "Margin Ratio": 1, "last nlv": 1}).limit(2).sort("YTD Return", -1)
        cursor2 = col.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1, "YTD Return": 1, "5 Yr Return": 1, "Margin Ratio": 1, "last nlv": 1}).limit(2).sort("YTD Return", 1)
        list_cur1 = list(cursor1)
        list_cur2 = list(cursor2)
        final = {}
        final["top"] = list_cur1
        final["bottom"] = list_cur2
        json_data = dumps(final)
        print(json_data)
        return json_data

    def all_algo_1c(self, tags=""):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        if tags == "popular":
            cursor = col.find({"tags":{"$in":["popular"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1, "last nlv":1})
        elif tags == "geo_focus":
            cursor = col.find({"tags":{"$in":["geo_focus"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1, "last nlv":1})
        elif tags == "votility_rider":
            cursor = col.find({"tags":{"$in":["votility_rider"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1, "last nlv":1})
        elif tags == "long_term_value":
            cursor = col.find({"tags":{"$in":["long_term_value"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1, "last nlv":1})
        elif tags == "drawdown_protection":
            cursor = col.find({"tags":{"$in":["drawdown_protection"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1, "last nlv":1})
        else:
            cursor = col.find({}, {"_id":0, "strategy_name":1, "strategy_initial":1, "last nlv":1})
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    def user_acc_2a(self):
        self.db = self.conn["simulation"]
        col = self.db.backtest_portfolio_rebalance_0
        cursor = col.find({}, {"_id": 0, "NetLiquidation": 1, "timestamp": 1}).sort("_id", 1)
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    def user_acc_2b(self, name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor = col.find({"strategy_name": name}, {"_id": 0, "Since Inception Return": 1, "1 Yr Sharpe": 1, "5 Yr Sharpe": 1, "YTD Sharpe": 1, "1 Yr Return": 1, "5 Yr Return": 1, "YTD Return": 1, "YTD Sortino": 1, "YTD Max Drawdown": 1, "YTD Volatility": 1, "YTD Win Rate": 1, "YTD Average Win Per Day": 1, "YTD Profit Loss Ratio": 1, "Margin Ratio": 1, "last nlv": 1}).limit(1)
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    def user_acc_2c(self, name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor = col.find({"strategy_name": name}, {"_id": 0, "Composite": 1}).limit(1)
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    def user_acc_2d(self, name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor = col.find({"strategy_name": name}, {"_id": 0, "Composite": 1,"strategy_name": 1}).limit(1)
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    def user_acc_2e(self, name=""):
        self.db = self.conn["Transactions"]
        col = self.db.Strategies
        cursor = col.find({"strategy_name": name}, {"_id": 0, "Composite": 1,"strategy_name": 1}).limit(1)
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    # def convert_df_to_json(self, df):
    #     json_file = df.to_json(orient='records')
    #     return json_file

def main():
    a = Api_Mongodb()
    # a.all_algo_1a()
    # a.all_algo_1b()
    # a.all_algo_1c("")
    # a.user_acc_2a()
    # a.user_acc_2b()
    # a.user_acc_2c()
    # a.user_acc_2d()
    a.user_acc_2e()
if __name__ == "__main__":
        main()
