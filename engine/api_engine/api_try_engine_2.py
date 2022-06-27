import certifi
from pymongo import MongoClient
from bson.json_util import dumps
import pandas as pd


class Api_Mongodb_2:
    db = ""
    conn = ""

    def __init__(self):
        try:
            self.conn = MongoClient(
                'mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',
                tlsCAFile=certifi.where())
            print("Successful connection to mongoClient")
        except:
            print("WARNING: Could not connect to MongoDB")

    # @app.route('/mainpage')
    def all_algo_4a(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        data = col.find({}, {"_id": 0, "Composite": 1}).sort("_id", 1)
        df = pd.DataFrame()
        percentage = list()
        name = list()
        for x in data:
            percentage.append(list(x["Composite"].values()))
            name.append(list(x["Composite"].keys()))
        df["ETF_percentage"] = percentage
        df["ETF_name"] = name

        json_data = self.convert_df_to_json(df)
        print(json_data)
        return json_data

    def all_algo_4b(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        data = col.find({}, {"_id": 0, "Composite": 1, "strategy_name": 1}).sort("_id", 1)
        df = pd.DataFrame()
        percentage = list()
        name = list()
        strategy = list()
        for x in data:
            percentage.append(list(x["Composite"].values()))
            name.append(list(x["Composite"].keys()))
            strategy.append(x["strategy_name"])
        df["weight"] = percentage
        df["ETF_ticker, ETF_name"] = name
        df["strategy_name"] = strategy
        json_data = self.convert_df_to_json(df)
        print(json_data)
        return json_data
    def all_algo_4c(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        data = col.find({}, {"_id": 0, "1 Year Return": 1, "3 Year Return": 1, "5 Year Return": 1, "YTD Return": 1,
                             "Since Inception Return": 1, "1 Yr adj return": 1,"3 Yr adj return": 1,
                             "5 Yr adj return": 1}).sort("_id", 1)
        json_data = dumps(data)
        print(json_data)
        return json_data


    def convert_df_to_json(self, df):
        json_file = df.to_json(orient='records')
        return json_file


def main():
    a = Api_Mongodb_2()
    a.all_algo_4a()
    a.all_algo_4b()
    a.all_algo_4c()



if __name__ == "__main__":
    main()
