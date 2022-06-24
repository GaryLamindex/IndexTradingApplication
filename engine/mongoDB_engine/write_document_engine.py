from engine.mongoDB_engine.mongodb_engine import mongodb_engine
import pandas as pd


class Write_Mongodb:

    mongo = None

    def __init__(self, _database = 'rainydrop'):
        self.mongo = mongodb_engine(_database)

    def write_Strategies(self, dict_list):
        for item in dict_list:
            if self.mongo.db['Strategies'].count_documents({'name': item['name']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("Strategies", item)

    def write_Clients(self, dict_list):
        for item in dict_list:
            if self.mongo.db['Clients'].count_documents({'name': item['name']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("Clients", item)

    def write_Traders(self, dict_list):
        for item in dict_list:
            if self.mongo.db['Traders'].count_documents({'name': item['name']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("Traders", item)

    def write_Transactions(self, dict_list):
        for item in dict_list:
            if self.mongo.db['Transactions'].count_documents({'_id': item['_id']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("Transactions", item)

    def write_ETFs(self, dict_list):
        for item in dict_list:
            if self.mongo.db['ETFs'].count_documents({'name': item['name']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("ETFs", item)

    def write_ETF_raw_data(self, dict_list):
        self.mongo.write_mongodb_dict_list("ETF_raw_data", dict_list)

    def write_Performance(self, dict_list, _name):
        if self.mongo.db['Performance'].count_documents({'name': _name['name']}) > 0:
            print("document already exist")
        else:
            for item in dict_list:
                self.mongo.write_mongodb_dict_list("Performance", item)

    def write_one_min_raw_data(self, _collection,dict_list):
        self.mongo.write_mongodb_many_dict_list(_collection, dict_list)

    def rename_collection(self,_collection, name):
        self.mongo.rename_collection(_collection,name)



def main():
    # w = Write_Mongodb('one_min_raw_data')
    # df = pd.read_csv('/Users/chansiuchung/Documents/IndexTrade/ticker_data/one_min/QQQ.csv')
    #
    # temp_list = df.to_dict(orient='records')
    # print("SUCCESS")
    # w.write_one_min_raw_data("QQQ",temp_list)

    # w = Write_Mongodb('one_min_raw_data')
    # df = pd.read_csv('/Users/chansiuchung/Documents/IndexTrade/ticker_data/one_min/SPY.csv')
    #
    # temp_list = df.to_dict(orient='records')
    # print("SUCCESS")
    # w.write_one_min_raw_data("SPY",temp_list)

    # w = Write_Mongodb('one_min_raw_data')
    # df = pd.read_csv('/Users/chansiuchung/Documents/IndexTrade/ticker_data/one_min/3188.csv')
    #
    # temp_list = df.to_dict(orient='records')
    # print("SUCCESS")
    # w.write_one_min_raw_data("3188",temp_list)

    # w = Write_Mongodb('drawdown_data')
    # df = pd.read_csv('/Users/chansiuchung/Documents/IndexTrade/user_id_0/backtest/backtest_rebalance_margin_wif_max_drawdown_control_0/stats_data/drawdown_abstract.csv')
    #
    # temp_list = df.to_dict(orient='records')
    # print("SUCCESS")
    # w.write_one_min_raw_data("backtest_rebalance_margin_wif_max_drawdown_control_0",temp_list)

    w = Write_Mongodb('rainydrop')
    #df = pd.read_csv('/Users/chansiuchung/Documents/IndexTrade/user_id_0/backtest/backtest_portfolio_rebalance_0/stats_data/all_file_return.csv')

    #temp_list = df.to_dict(orient='records')
    print("SUCCESS")
    #w.write_one_min_raw_data("Strategies", temp_list)
    w.mongo.db.Strategies.update_many({"Backtest Spec":"50_M_50_MSFT_"},{"$set":{"Rating.next20_portfolio":3.3}})
    w.mongo.db.Strategies.update_many({"Backtest Spec": "20_M_80_MSFT_"},
                                      {"$set": {"Rating.future_tech_portfolio": 5}})



    # w = Write_Mongodb('simulation')
    # w.rename_collection('40_M_60_MSFT_','backtest_portfolio_rebalance_0')
    # w.rename_collection('50_VTV_50_SPY_','backtest_portfolio_rebalance_1')
    # df = pd.read_csv('/Users/chansiuchung/Downloads/M MSFT/VTV SPY/50_VTV_50_SPY_.csv')
    #
    # temp_list = df.to_dict(orient='records')
    # print("SUCCESS")
    # w.write_one_min_raw_data("50_VTV_50_SPY_",temp_list)

    #
    # client_data = {"name": ["Ivy", "Peter", "Percy"], "VIP":["Premium", "basic","Gold+"], "Followed":[[3,5],[6,5,2,1],[0,5,8,9,45]]}
    # client_df = pd.DataFrame(data=client_data)
    # dict = client_df.to_dict(orient='records')
    # w.write_Clients(dict)
    #
    # Trader_data = {"name": ["V. Poor", "N. Win", "Not Warrent Buffet"], "VIP": ["Premium", "basic", "Gold+"],
    #                "return":[0.8, 3.4, 7.2],
    #                "rating": [3.4, 7, 8.4]}
    # Trader_df = pd.DataFrame(data=Trader_data)
    # dict = Trader_df.to_dict(orient='records')
    # w.write_Traders(dict)
    #
    # strategies_data = {"name": ["buffet50", "ark kiss", "poor"], "Return":[5,7,0.2]}
    # strategies_df = pd.DataFrame(data=strategies_data)
    # _dict = strategies_df.to_dict(orient='records')
    # w.write_Strategies(_dict)
    #
    return

if __name__ == "__main__":
    main()