from IndexTradingApplication.engine.mongoDB_engine.mongodb_engine import mongodb_engine
from flask import Flask, app
from flask_restful import Api, Resource

# @app.route('/mainpage')
def all_algo_1a():
    mongo = mongodb_engine("simulation")
    col = mongo.db.backtest_portfolio_rebalance_0
    data = col.find({}, {"_id": 0, "NetLiquidation": 1, "timestamp": 1}).sort("_id", 1)
    for x in data: print(x)
    return data

def all_algo_1b():
    mongo = mongodb_engine()
    col = mongo.db.Strategies
    data1 = col.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1, "YTD Return": 1, "5 Yr Return": 1, "Margin Ratio": 1}).limit(2).sort("YTD Return", -1)
    data2 = col.find({}, {"_id": 0, "strategy_name": 1, "YTD Sharpe": 1, "5 Yr Sharpe": 1, "YTD Return": 1, "5 Yr Return": 1, "Margin Ratio": 1}).limit(2).sort("YTD Return", 1)
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
    final ["bottom"] = temp2
    print(final)
    # for x in data2: print(x)
    # print({"top": data1, "bottom": data2})
    return {data1, data2}

def convert_df_to_json(self, df):
    json_file = df.to_json(orient='records')
    return json_file

def main():
    # all_algo_1a()
    all_algo_1b()

if __name__ == "__main__":
    main()