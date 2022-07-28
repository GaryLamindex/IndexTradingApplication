import json
from pymongo import MongoClient
import certifi
import requests
import pandas as pd
from bson.json_util import loads

class Write_Mongodb:
    nft_flask = 'nft-flask'
    watchlist_suggestions = 'watchlistSuggestions'
    trading_cards = 'tradingCards'
    strategyequity = 'strategyEquity'
    rollingReturns = 'rollingReturns'
    algoPrincipleTop = 'algoPrincipleTop'
    rainydrop = 'rainydrop'
    Strategies = 'Strategies'
    drawdown_data = 'drawdown_data'
    simulation = 'simulation'
    drawdown_graph_data = 'drawdown_graph_data'

    def __init__(self, run_data_df=None, all_file_return_df=None, drawdown_abstract_df=None, drawdown_raw_df=None):
        self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=certifi.where())
        print(f"Successful connection to mongoClient")
        self.rainydrop_db = self.conn[self.rainydrop]
        self.nft_db = self.conn[self.nft_flask]
        self.drawdown_data_db = self.conn[self.drawdown_data]
        self.simulation_db = self.conn[self.simulation]
        self.drawdown_graph_data_db = self.conn[self.drawdown_graph_data]
        self.nft_db = self.conn[self.nft_flask]
        self.run_data_df = run_data_df
        self.all_file_return_df = all_file_return_df
        self.drawdown_abstract_df = drawdown_abstract_df
        self.drawdown_raw_df = drawdown_raw_df
        self.mongo = None

    # def write_AssetAllocationAndETFs(self, all_file_return_df):
    #     # write AssetAllocationAndETFs collection in nft-flask
    #     self.db = self.conn[self.nft_flask]
    #     coll = self.db[self.asset_allocation_ETFs]
    #     # print(asset_ETF_df)
    #     for x in range(len(all_file_return_df)):
    #         # print(asset_ETF_df["Backtest Spec"][x])
    #         if coll.count_documents({"Backtest Spec": all_file_return_df["Backtest Spec"][x]}) > 0:
    #             print(f'document for {all_file_return_df["Backtest Spec"][x]} already exist in AssetAllocationAndETFs')
    #         else:
    #             string = all_file_return_df["Composite"][x]
    #             json_acceptable_string = string.replace("'", "\"")
    #             composite_dict = loads(json_acceptable_string)
    #             # print(composite_dict)
    #             for y in composite_dict:
    #                 asset_ETF = {"strategy_name": all_file_return_df["Backtest Spec"][x], "ETF_percentage": composite_dict[y], "ETF_label": y}
    #                 print(asset_ETF)
    #                 # coll.insert_one(asset_ETF)
    #     return

    def overview_bottom_loser(self):
        coll = self.rainydrop_db[self.Strategies]
        document = coll.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1,
                                "YTD Return": 1, "5 Yr Return": 1, "Margin Ratio": 1,
                                "last nlv": 1}).limit(5).sort("YTD Return", -1)
        for x in document:
            print(x)
        return

    def overview_top_gainer(self):
        coll = self.rainydrop_db[self.Strategies]
        document = coll.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1,
                                "YTD Return": 1, "5 Yr Return": 1, "Margin Ratio": 1,
                                "last nlv": 1}).limit(5).sort("YTD Return", 1)
        for x in document:
            print(x)
        return

def main():
    w = Write_Mongodb()
    w.overview_bottom_loser()
    w.overview_top_gainer()
    # all_file_return_df = pd.read_csv('/Users/ching/Downloads/all_file_return 2.csv')
    # w.write_AssetAllocationAndETFs(all_file_return_df)


if __name__ == "__main__":
    main()