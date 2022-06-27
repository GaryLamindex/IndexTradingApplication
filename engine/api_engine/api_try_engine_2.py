import certifi
from pymongo import MongoClient

class Api_Mongodb_2:
    db =""
    conn=""
    def __init__(self):
        try:
            self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/'
                                    '?retryWrites=true&w=majority',tlsCAFile=certifi.where())
            print(f"Successful connection to database: {self.db.name}")
        except:
            print("WARNING: Could not connect to MongoDB")

    # @app.route('/mainpage')
    def all_algo_4a(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        data = col.find({}, {"_id": 0, "1 Yr Return": 1}).sort("_id", 1)
        for x in data:
            print(x)
        return data
    def all_algo_1a(self):
        self.db = self.conn["simulation"]
        col = self.db.backtest_portfolio_rebalance_0
        data = col.find({}, {"_id": 0, "NetLiquidation": 1, "timestamp": 1}).sort("_id", 1)
        for x in data: print(x)
        return data
    def convert_df_to_json(self, df):
        json_file = df.to_json(orient='records')
        return json_file

def main():
    a = Api_Mongodb_2()
    a.all_algo_1a()
    # a.all_algo_4a()
if __name__ == "__main__":
        main()
