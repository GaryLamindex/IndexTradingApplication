import certifi
from pymongo import MongoClient
from bson.json_util import dumps, loads
import pandas as pd

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
    # same as user_acc_2a indv_algo_3b
    def all_algo_1a(self, strategy_name=["backtest_portfolio_rebalance_0","backtest_portfolio_rebalance_1"], timestamp_start=1262275200, timestamp_end=1285935780):
        self.db = self.conn["simulation"]
        all = {}
        for x in strategy_name:
            col = self.db[x]
            cursor = col.find({"timestamp": {"$lte": timestamp_end,"$gte": timestamp_start}}, {"_id": 0, "NetLiquidation": 1, "timestamp": 1}).sort("_id", 1)
            list_ = list(cursor)
            # print(list_)
            all[x] = list_
        json_data = dumps(all)
        print(json_data)
        return json_data

    def all_algo_1b(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor1 = col.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1,
                                "YTD Return": 1, "5 Yr Return": 1, "Margin Ratio": 1,
                                "last nlv": 1}).limit(2).sort("YTD Return", -1)
        cursor2 = col.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1,
                                "YTD Return": 1, "5 Yr Return": 1, "Margin Ratio": 1,
                                "last nlv": 1}).limit(2).sort("YTD Return", 1)
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
            cursor = col.find({"tags":{"$in":["popular"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1,
                                                             "last daily change":1, "last monthly change": 1})
        elif tags == "geo_focus":
            cursor = col.find({"tags":{"$in":["geo_focus"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1,
                                                               "last daily change":1, "last monthly change": 1})
        elif tags == "votility_rider":
            cursor = col.find({"tags":{"$in":["votility_rider"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1,
                                                                    "last daily change":1, "last monthly change": 1})
        elif tags == "long_term_value":
            cursor = col.find({"tags":{"$in":["long_term_value"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1,
                                                                     "last daily change":1, "last monthly change": 1})
        elif tags == "drawdown_protection":
            cursor = col.find({"tags":{"$in":["drawdown_protection"]}}, {"_id":0, "strategy_name":1, "strategy_initial":1,
                                                                         "last daily change":1, "last monthly change": 1})
        else:
            cursor = col.find({}, {"_id":0, "strategy_name":1, "strategy_initial":1, "last daily change":1, "last monthly change": 1})
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    # same as all_algo_1a() indv_algo_3b
    def user_acc_2a(self, strategy_name=["backtest_portfolio_rebalance_0","backtest_portfolio_rebalance_1"], timestamp_start=1262275200, timestamp_end=1298937600):
        self.db = self.conn["simulation"]
        all = {}
        for x in strategy_name:
            col = self.db[x]
            cursor = col.find({"timestamp": {"$lte": timestamp_end, "$gte": timestamp_start}},
                              {"_id": 0, "NetLiquidation": 1, "timestamp": 1}).sort("_id", 1)
            list_ = list(cursor)
            # print(list_)
            all[x] = list_
        json_data = dumps(all)
        print(json_data)
        return json_data

    def user_acc_2b(self, strategy_name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor = col.find({"strategy_name": strategy_name}, {"_id": 0,"Since Inception Return": 1, "1 Yr Sharpe": 1,
                                                             "5 Yr Sharpe": 1, "Since Inception Sharpe": 1, "1 Yr Return":1,
                                                             "5 Yr Return":1, "YTD Return":1,
                                                             "Since Inception Sortino": 1, "Since Inception Max Drawdown":1,
                                                             "Since Inception Volatility":1,"Since Inception Win Rate": 1,
                                                             "Since Inception Average Win Per Day":1, "Since Inception Profit Loss Ratio":1,
                                                             "last nlv":1, "Margin Ratio": 1}).limit(1)
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    def user_acc_2c(self, strategy_name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor = col.find({"strategy_name": strategy_name}, {"_id": 0, "Composite": 1}).limit(1)
        df = pd.DataFrame()
        for x in cursor:
            percentage = list(x["Composite"].values())
            ETF = list(x["Composite"].keys())
        df["weight"] = percentage
        df["ETF_ticker, ETF_name"] = ETF
        json_data = self.convert_df_to_json(df)
        print(json_data)
        return json_data

    def user_acc_2d(self, strategy_name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor = col.find({"strategy_name": strategy_name}, {"_id": 0, "Composite": 1,"strategy_name": 1}).limit(1)
        df = pd.DataFrame()
        for x in cursor:
            percentage = list(x["Composite"].values())
            ETF = list(x["Composite"].keys())
            strategy= x["strategy_name"]
        df["weight"] = percentage
        df["ETF_ticker, ETF_name"] = ETF
        df["strategy_name"] = strategy
        json_data = self.convert_df_to_json(df)
        print(json_data)
        return json_data

    def user_acc_2e(self, client_name="neoculturetech"):
        self.db = self.conn["rainydrop"]
        col = self.db.Clients
        cursor = col.find({"client_name": client_name}, {"_id": 0, "transactions": 1}).limit(1)
        transac_list = cursor[0]["transactions"]
        # print(transac_list)
        col = self.db.Transactions
        cursor = col.find({"transaction_id": {"$in": transac_list}}, {"_id": 0, "date_time": 1, "ETF_ticker": 1,
                                                                      "action": 1, "price": 1, "quantity": 1,
                                                                      "total_amount": 1, "strategy_name": 1})
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    def user_acc_2f(self, ETF_ticker="3188"):
        self.db = self.conn["rainydrop"]
        col = self.db.ETF
        cursor1 = col.find({"ETF_ticker": ETF_ticker}, {"_id": 0, "ETF_ticker": 1, "ETF_name": 1, "price": 1,
                                                        "price_change": 1, "price_%change": 1, "volatility": 1,
                                                        "return": 1, "dividend_yield": 1, "price_earnings": 1,
                                                        "field": 1}).limit(1)
        self.db = self.conn["one_min_raw_data"]
        col = self.db[ETF_ticker]
        cursor2 = col.find({}, {"_id": 0, "timestamp": 1, "close": 1}).limit(10)  #limit 10 for testing
        list_cur1 = list(cursor1)
        list_cur2 = list(cursor2)
        final = {}
        final["details"] = list_cur1
        final["graph"] = list_cur2
        json_data = dumps(final)
        print(json_data)
        return json_data

    def indv_algo_3a(self, strategy_name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor = col.find({"strategy_name": strategy_name}, {"_id": 0, "Since Inception Return": 1, "Since Inception Sharpe": 1,
                                                             "Margin Ratio": 1, "last nlv": 1}).limit(1)
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    # same as all_algo_1a() user_acc_2a()
    def indv_algo_3b(self, strategy_name=["backtest_portfolio_rebalance_0"], timestamp_start=1262275200, timestamp_end=1298937600):
        self.db = self.conn["simulation"]
        all = {}
        for x in strategy_name:
            col = self.db[x]
            cursor = col.find({"timestamp": {"$lte": timestamp_end, "$gte": timestamp_start}},
                              {"_id": 0, "NetLiquidation": 1, "timestamp": 1}).sort("_id", 1)
            list_ = list(cursor)
            # print(list_)
            all[x] = list_
        json_data = dumps(all)
        print(json_data)
        return json_data

    def indv_algo_3m(self, strategy_name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor1 = col.find({"strategy_name": strategy_name}, {"_id": 0, "video_link": 1, "documents_link": 1, "trader_name": 1}).limit(1)
        df = pd.DataFrame()
        video_link = list()
        documents_link = list()
        trader_name = list()
        for x in cursor1:
            video_link.append(x["video_link"])
            documents_link.append(x["documents_link"])
            trader_name.append(x["trader_name"])
        col = self.db.Traders
        cursor2 = col.find({"trader_name": trader_name[0]}, {"_id": 0, "trader_info": 1})
        trader_info = list()
        for x in cursor2:
            trader_info.append((x["trader_info"]))
        df["video_link"] = video_link
        df["documents_link"] = documents_link
        df["trader_name"] = trader_name
        df["trader_info"] = trader_info
        json_data = self.convert_df_to_json(df)
        print(json_data)
        return json_data

    def indv_algo_3g(self, strategy_name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor1 = col.find({"strategy_name": strategy_name},{"_id": 0, "Composite": 1})
        df = pd.DataFrame()
        for x in cursor1:
            percentage = list(x["Composite"].values())
            ETF = list(x["Composite"].keys())
        # print(percentage,ETF)
        col = self.db.ETF
        div_yield_current_year = list()
        div_yield_last_year = list()
        for x in ETF:
            cursor2 = col.find({"label": x},{"_id": 0, "div_yield_current_year": 1, "div_yield_last_year": 1})
            for x in cursor2:
                div_yield_current_year.append(x["div_yield_current_year"])
                div_yield_last_year.append(x["div_yield_last_year"])
        # print(div_yield_current_year,div_yield_last_year)
        df["ETF"] = ETF
        df["percentage"] = percentage
        df["div_yield_current_year"] = div_yield_current_year
        df["div_yield_last_year"] = div_yield_last_year
        json_data = self.convert_df_to_json(df)
        print(json_data)
        return json_data

    def indv_algo_3h(self, strategy_name="backtest_rebalance_margin_wif_max_drawdown_control_0"):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        cursor = col.find({"strategy_name": strategy_name},{"_id": 0, "1 Yr Return": 1, "3 Yr Return": 1, "5 Yr Return": 1,
                                                            "YTD Return": 1, "Since Inception Return": 1, "1 Yr adj return": 1,
                                                            "3 Yr adj return": 1, "5 Yr adj return": 1, "YTD adj Return": 1,
                                                            "Since Inception adj Return": 1, "YTD Max Drawdown": 1,
                                                            "1 Yr Max Drawdown": 1, "3 Yr Max Drawdown": 1,
                                                            "5 Yr Max Drawdown": 1, "Since Inception Max Drawdown": 1})
        json_data = dumps(cursor)
        print(json_data)
        return json_data

    def convert_df_to_json(self, df):
        json_file = df.to_json(orient='records')
        return json_file

def main():
    a = Api_Mongodb()
    # a.all_algo_1a()
    # a.all_algo_1b()
    # a.all_algo_1c()
    # a.user_acc_2a()
    # a.user_acc_2b()
    # a.user_acc_2c()
    # a.user_acc_2d()
    # a.user_acc_2e("city127")
    # a.user_acc_2f()
    # a.indv_algo_3a()
    # a.indv_algo_3b()
    # a.indv_algo_3m()
    # a.indv_algo_3g()
    # a.indv_algo_3h()

if __name__ == "__main__":
        main()