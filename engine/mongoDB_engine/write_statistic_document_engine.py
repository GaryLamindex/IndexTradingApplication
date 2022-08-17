import json

from pymongo import MongoClient
import certifi
import requests
import pandas as pd


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

    def __init__(self):
        self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=certifi.where())
        print(f"Successful connection to mongoClient")
        # except:
        #     print("WARNING: Could not connect to MongoDB")

    def write_drawdown_data(self, strategy_name, drawdown_abstract_df):
        """write drawdown_data database, plesase store abstract data rather than raw data """
        self.db = self.conn[self.drawdown_data]
        drawdown_abstract_records = drawdown_abstract_df.to_dict(orient='records')
        coll = self.db[strategy_name]
        for x in drawdown_abstract_records:
            coll.replace_one({'Drawdown period': x['Drawdown period']}, x, upsert=True)
        return

    def write_drawdown_graph_data(self, strategy_name, drawdown_raw_df):
        """write drawdown_graph_data database, please use raw data but not abstract data"""
        self.db = self.conn[self.drawdown_graph_data]
        drawdown_raw_records = drawdown_raw_df.to_dict(orient='records')
        coll = self.db[strategy_name]
        for x in drawdown_raw_records:
            coll.replace_one({'timestamp': x['timestamp']}, x, upsert=True)
        return


    def write_strategies(self, strategy_name=None, all_file_return_df=None, strategy_initial=None, video_link=None, documents_link=None,
                         tags_array=None, rating_dict=None, margin_ratio=None, subscribers_num=None, trader_name=None, name = None):
        """write Strategies collection in rainydrop"""
        self.db = self.conn[self.rainydrop]
        coll = self.db[self.strategies]
        all_file_return_df['strategy_initial'] = strategy_initial
        all_file_return_df['strategy_name'] = strategy_name
        all_file_return_df['video_link'] = video_link
        all_file_return_df['documents_link'] = documents_link
        all_file_return_df['tags_array'] = tags_array
        all_file_return_df['rating_dict'] = rating_dict
        all_file_return_df['subscribers num'] = subscribers_num
        all_file_return_df['margin ratio'] = margin_ratio
        all_file_return_df['subscribers_num'] = subscribers_num
        all_file_return_df['trader_name'] = trader_name
        all_file_return_df = all_file_return_df.loc[all_file_return_df['Backtest Spec'] == name]
        all_file_return_record = all_file_return_df.to_dict(orient='records')
        for x in all_file_return_record:
            coll.replace_one({'strategy_name': x['strategy_name']}, x, upsert=True)
        return

    def write_ETF(self, ETF_name, ETF_df):
        """write ETF collection in rainydrop"""
        self.db = self.conn[self.rainydrop]
        coll = self.db[self.ETF]
        if coll.count_documents({'ETF_name': ETF_name}) > 0:
            print('document already exist in ETF')
        else:
            ETF_records = ETF_df.to_dict(orient='records')
            coll.insert_many(ETF_records)
        return

    def write_Traders(self, trader_id, Traders_df):
        """write Traders collection in rainydrop, enter trader id to do validation"""
        self.db = self.conn[self.rainydrop]
        coll = self.db[self.traders]
        if coll.count_documents({'trader_id': trader_id}) > 0:
            print('document already exist in Traders')
        else:
            trader_records = Traders_df.to_dict(orient='records')
            coll.insert_many(trader_records)
        return

    def write_Clients(self, client_id, client_df):
        """write Clients collection in rainydrop"""
        self.db = self.conn[self.rainydrop]
        coll = self.db[self.clients]
        if coll.count_documents({'client_id': client_id}) > 0:
            print('document already exist in Clients')
        else:
            client_records = client_df.to_dict(orient='records')
            coll.insert_many(client_records)
        return

    def write_Transactions(self, transaction_id, transaction_df):
        """write Client collection in rainydrop"""
        self.db = self.conn[self.rainydrop]
        coll = self.db[self.transactions]
        if coll.count_documents({'transaction_id': transaction_id}) > 0:
            print('document already exist in transactions')
        else:
            transaction_records = transaction_df.to_dict(orient='records')
            coll.insert_many(transaction_records)
        return

    def write_new_backtest_result(self, strategy_name, drawdown_abstract_df, drawdown_raw_df, all_file_return_df,
                                  strategy_initial, video_link, documents_link, tags_array, rating_dict, margin_ratio,
                                  subscribers_num, trader_name, name):
        """call whenever upload new backtest data, initiate everything"""
        self.write_drawdown_data(strategy_name, drawdown_abstract_df)
        self.write_drawdown_graph_data(strategy_name, drawdown_raw_df)
        self.write_strategies(strategy_name, all_file_return_df, strategy_initial, video_link, documents_link,
                              tags_array, rating_dict, margin_ratio, subscribers_num, trader_name, name)
        return



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

    # w = Write_Mongodb()
    # #df = pd.read_csv('/Users/chansiuchung/Documents/IndexTrade/user_id_0/backtest/backtest_portfolio_rebalance_0/stats_data/all_file_return.csv')
    #
    # #temp_list = df.to_dict(orient='records')
    # w.db = w.conn['rainydrop']
    #
    # w.coll = w.db['Strategies']
    # print("SUCCESS")
    #w.write_one_min_raw_data("Strategies", temp_list)
    # w.coll.update_many({},{"$set":{"last daily change":0.02567}})
    # w.coll.update_many({}, {"$set": {"last monthly change": 0.14987567}})
    # w.mongo.db.Strategies.update_many({"Backtest Spec": "20_M_80_MSFT_"},
    #                                   {"$set": {"Rating.future_tech_portfolio": 5}})
    # w.coll.update_many({},{"$set":{"trader_name": ""}})


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

    data = json.dumps({'ETF_percentage': 0.2391,'ETF_label': 'HELLO'})
    requests.post('http://127.0.0.1:5000/composite/asset-allocation-etfs', json=data)
    return

if __name__ == "__main__":
    main()