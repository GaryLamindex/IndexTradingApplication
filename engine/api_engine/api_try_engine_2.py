import certifi
from pymongo import MongoClient


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
    def all_algo_1b(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        data1 = col.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1, "YTD Return": 1,
                              "5 Yr Return": 1, "Margin Ratio": 1}).limit(2).sort("YTD Return", -1)
        data2 = col.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1, "YTD Return": 1,
                              "5 Yr Return": 1, "Margin Ratio": 1}).limit(2).sort("YTD Return", 1)
        temp1 = []
        for x in data1:
            print(x)
            temp1.append(x)
        temp2 = []
        for x in data2:
            print(x)
            temp2.append(x)
        final = {}
        final["top"] = temp1
        final["bottom"] = temp2
        print(final)
        # for x in data2: print(x)
        # print({"top": data1, "bottom": data2})
        return {data1, data2}

    def all_algo_4a(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        data = col.find({}, {"_id": 0, "Composite": 1}).sort("_id", 1)
        output = []
        for x in data:
            output.append(list(x["Composite"].values()) + list(x["Composite"].keys()))
        print(output)
        return output

    def all_algo_4b(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        data = col.find({}, {"_id": 0, "Composite": 1, "strategy_name": 1}).sort("_id", 1)
        output = []
        for x in data:
            a = list(x["Composite"].values()) + list(x["Composite"].keys())
            a.append(x["strategy_name"])
            output.append(a)

        print(output)
        return output

    def all_algo_4c(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        data = col.find({}, {"_id": 0, "1 Year Return": 1, "3 Year Return": 1, "5 Year Return": 1, "YTD Return": 1,
                             "Since Inception Return": 1, "1 Yr adj return": 1,"3 Yr adj return": 1,
                             "5 Yr adj return": 1}).sort("_id", 1)
        output = []
        for x in data:
            output.append(x)
        print(output)
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
