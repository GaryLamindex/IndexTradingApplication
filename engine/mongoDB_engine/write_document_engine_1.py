import json
from pymongo import MongoClient
import certifi
import requests
import pandas as pd
from bson.json_util import loads

class Write_Mongodb:
    """
    call the constructor of this class in order to connect to garylam mongoDB server
    call write_new_backtest_result() for uploading new backtest results
    """

    #for connection
    mongo = None
    conn = None
    db = None

    # change the string for the database and collection if necessary here (e.g. renamed)
    drawdown_data = "drawdown_data"
    drawdown_graph_data = "drawdown_graph_data"
    simulation = "simulation"
    rainydrop = "rainydrop"
    strategies = "Strategies"
    ETF = "ETF"
    traders = "Traders"
    clients = "Clients"
    transactions = "Transactions"
    nft_flask = "nft-flask"
    asset_allocation_ETFs = "AssetAllocationAndETFs"

    def __init__(self):
        self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=certifi.where())
        print(f"Successful connection to mongoClient")
        # except:
        #     print("WARNING: Could not connect to MongoDB")

    def write_AssetAllocationAndETFs(self, asset_ETF_df):
        # write AssetAllocationAndETFs collection in nft-flask
        self.db = self.conn[self.nft_flask]
        coll = self.db[self.asset_allocation_ETFs]
        # print(asset_ETF_df)
        for x in range(len(asset_ETF_df)):
            # print(asset_ETF_df["Backtest Spec"][x])
            if coll.count_documents({"Backtest Spec": asset_ETF_df["Backtest Spec"][x]}) > 0:
                print(f'document for {asset_ETF_df["Backtest Spec"][x]} already exist in AssetAllocationAndETFs')
            else:
                string = asset_ETF_df["Composite"][x]
                json_acceptable_string = string.replace("'", "\"")
                composite_dict = loads(json_acceptable_string)
                # print(composite_dict)
                for y in composite_dict:
                    asset_ETF = {"strategy_name": asset_ETF_df["Backtest Spec"][x], "ETF_percentage": composite_dict[y], "ETF_label": y}
                    print(asset_ETF)
                    coll.insert_one(asset_ETF)
                    return
        return

def main():
    w = Write_Mongodb()
    df = pd.read_csv('/Users/ching/Downloads/all_file_return 2.csv')
    w.write_AssetAllocationAndETFs(df)
    # w.db = w.conn[w.nft_flask]
    # w.coll = w.db[w.asset_allocation_ETFs]
    # w.coll.delete_many({"last daily change": 0})

if __name__ == "__main__":
    main()