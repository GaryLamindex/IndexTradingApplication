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
    def indv_algo_3b(self):
        self.db = self.conn["simulation"]
        col = self.db.backtest_portfolio_rebalance_0
        data = col.find({}, {"_id": 0, "timestamp": 1, "NetLiquidation": 1}).sort("_id", 1)
        json_data = dumps(data)
        print(json_data)
        return json_data

    def indv_algo_3d(self):
        self.db = self.conn["drawdown_graph_data"]
        col = self.db.backtest_portfolio_rebalance_0
        data = col.find({}, {"_id": 0, "timestamp": 1, "drawdown": 1}).sort("_id", 1)
        json_data = dumps(data)
        print(json_data)
        return json_data

    def indv_algo_3e(self):
        self.db = self.conn["drawdown_data"]
        col = self.db.backtest_portfolio_rebalance_0
        data = col.find({}, {"_id": 0, "Drawdown": 1, "Drawdown period": 1, "Drawdown days": 1, "Recovery date": 1,
                             "Recovery days": 1}).sort("_id", 1)
        json_data = dumps(data)
        print(json_data)
        return json_data

    def indv_algo_3f(self):
        self.db = self.conn["rainydrop"]
        col = self.db.Strategies
        data = col.find({}, {"_id": 0, "Since Inception Return": 1,"1 Yr Rolling Return": 1, "2 Yr Rolling Return": 1,
                             "3 Yr Rolling Return": 1, "5 Yr Rolling Return": 1, "7 Yr Rolling Return": 1,
                             "10 Yr Rolling Return": 1, "15 Yr Rolling Return": 1,
                             "20 Yr Rolling Return": 1}).sort("_id", 1)
        df = pd.DataFrame()
        one_yr_avg = list()
        two_yr_avg = list()
        three_yr_avg = list()
        five_yr_avg = list()
        seven_yr_avg = list()
        ten_yr_avg = list()
        fifteen_yr_avg = list()
        twenty_yr_avg = list()
        one_yr_max = list()
        two_yr_max = list()
        three_yr_max = list()
        five_yr_max = list()
        seven_yr_max = list()
        ten_yr_max = list()
        fifteen_yr_max = list()
        twenty_yr_max = list()
        one_yr_min = list()
        two_yr_min = list()
        three_yr_min = list()
        five_yr_min = list()
        seven_yr_min = list()
        ten_yr_min = list()
        fifteen_yr_min = list()
        twenty_yr_min = list()
        one_yr_neg_periods = list()
        two_yr_neg_periods = list()
        three_yr_neg_periods = list()
        five_yr_neg_periods = list()
        seven_yr_neg_periods = list()
        ten_yr_neg_periods = list()
        fifteen_yr_neg_periods = list()
        twenty_yr_neg_periods = list()

        for x in data:
            one_yr_avg.append(x["1 Yr Rolling Return"].get("average_annual_return"))
            two_yr_avg.append(x["2 Yr Rolling Return"].get("average_annual_return"))
            three_yr_avg.append(x["3 Yr Rolling Return"].get("average_annual_return"))
            five_yr_avg.append(x["5 Yr Rolling Return"].get("average_annual_return"))
            seven_yr_avg.append(x["7 Yr Rolling Return"].get("average_annual_return"))
            ten_yr_avg.append(x["10 Yr Rolling Return"].get("average_annual_return"))
            fifteen_yr_avg.append(x["15 Yr Rolling Return"].get("average_annual_return"))
            twenty_yr_avg.append(x["20 Yr Rolling Return"].get("average_annual_return"))
            one_yr_max.append(x["1 Yr Rolling Return"].get("max_annual_rolling_return"))
            two_yr_max.append(x["2 Yr Rolling Return"].get("max_annual_rolling_return"))
            three_yr_max.append(x["3 Yr Rolling Return"].get("max_annual_rolling_return"))
            five_yr_max.append(x["5 Yr Rolling Return"].get("max_annual_rolling_return"))
            seven_yr_max.append(x["7 Yr Rolling Return"].get("max_annual_rolling_return"))
            ten_yr_max.append(x["10 Yr Rolling Return"].get("max_annual_rolling_return"))
            fifteen_yr_max.append(x["15 Yr Rolling Return"].get("max_annual_rolling_return"))
            twenty_yr_max.append(x["20 Yr Rolling Return"].get("max_annual_rolling_return"))
            one_yr_min.append(x["1 Yr Rolling Return"].get("min_annual_rolling_return"))
            two_yr_min.append(x["2 Yr Rolling Return"].get("min_annual_rolling_return"))
            three_yr_min.append(x["3 Yr Rolling Return"].get("min_annual_rolling_return"))
            five_yr_min.append(x["5 Yr Rolling Return"].get("min_annual_rolling_return"))
            seven_yr_min.append(x["7 Yr Rolling Return"].get("min_annual_rolling_return"))
            ten_yr_min.append(x["10 Yr Rolling Return"].get("min_annual_rolling_return"))
            fifteen_yr_min.append(x["15 Yr Rolling Return"].get("min_annual_rolling_return"))
            twenty_yr_min.append(x["20 Yr Rolling Return"].get("min_annual_rolling_return"))
            one_yr_neg_periods.append(x["1 Yr Rolling Return"].get("negative_periods"))
            two_yr_neg_periods.append(x["2 Yr Rolling Return"].get("negative_periods"))
            three_yr_neg_periods.append(x["3 Yr Rolling Return"].get("negative_periods"))
            five_yr_neg_periods.append(x["5 Yr Rolling Return"].get("negative_periods"))
            seven_yr_neg_periods.append(x["7 Yr Rolling Return"].get("negative_periods"))
            ten_yr_neg_periods.append(x["10 Yr Rolling Return"].get("negative_periods"))
            fifteen_yr_neg_periods.append(x["15 Yr Rolling Return"].get("negative_periods"))
            twenty_yr_neg_periods.append(x["20 Yr Rolling Return"].get("negative_periods"))
        avglist = [one_yr_avg, two_yr_avg, three_yr_avg, five_yr_avg, seven_yr_avg, ten_yr_avg, fifteen_yr_avg, twenty_yr_avg]
        bestlist = [one_yr_max, two_yr_max, three_yr_max, five_yr_max, seven_yr_max, ten_yr_max, fifteen_yr_max, twenty_yr_max]
        worstlist = [one_yr_min, two_yr_min, three_yr_min , five_yr_min, seven_yr_min, ten_yr_min, fifteen_yr_min, twenty_yr_min]
        neglist = [one_yr_neg_periods, two_yr_neg_periods, three_yr_neg_periods, five_yr_neg_periods, seven_yr_neg_periods, ten_yr_neg_periods, fifteen_yr_neg_periods, twenty_yr_neg_periods]
        final_avglist = list()
        final_bestlist = list()
        final_worstlist = list()
        final_neglist = list()
        tmp_list = list()
        for x in range(0, len(one_yr_avg)):
            for y in range(0, len(avglist)):
                tmp_list.append(avglist[y][x])
            final_avglist.append(tmp_list.copy())
            tmp_list.clear()
        for x in range(0, len(one_yr_max)):
            for y in range(0, len(bestlist)):
                tmp_list.append(bestlist[y][x])
            final_bestlist.append(tmp_list.copy())
            tmp_list.clear()
        for x in range(0, len(one_yr_min)):
            for y in range(0, len(worstlist)):
                tmp_list.append(worstlist[y][x])
            final_worstlist.append(tmp_list.copy())
            tmp_list.clear()
        for x in range(0, len(one_yr_neg_periods)):
            for y in range(0, len(neglist)):
                tmp_list.append(neglist[y][x])
            final_neglist.append(tmp_list.copy())
            tmp_list.clear()
        rolling_list = ["1 Year", "2 Years", "3 Years", "5 Years", "7 Years", "10 Years", "15 Years",
                        "20 Years"]
        rolling_list = list(map(lambda b: rolling_list, one_yr_avg))
        df["Rolling Period"] = rolling_list
        df["Average(%)"] = final_avglist
        df["Best(%)"] = final_bestlist
        df["Worst(%)"] = final_worstlist
        df["Negative Periods"] = final_neglist
        json_data = self.convert_df_to_json(df)
        print(json_data)
        return json_data



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
                             "Since Inception Return": 1, "1 Yr adj return": 1, "3 Yr adj return": 1,
                             "5 Yr adj return": 1}).sort("_id", 1)
        json_data = dumps(data)
        print(json_data)
        return json_data

    def convert_df_to_json(self, df):
        json_file = df.to_json(orient='records')
        return json_file


def main():
    a = Api_Mongodb_2()
    a.indv_algo_3b()
    a.indv_algo_3d()
    a.indv_algo_3e()
    a.indv_algo_3f()
    a.all_algo_4a()
    a.all_algo_4b()
    a.all_algo_4c()



if __name__ == "__main__":
    main()
