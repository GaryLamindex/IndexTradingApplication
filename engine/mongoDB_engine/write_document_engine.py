from pymongo import MongoClient
import certifi
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
        try:
            self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',
                                    tlsCAFile=certifi.where())
            print(f"Successful connection to mongoClient")
        except:
            print("WARNING: Could not connect to MongoDB")

    # def rename_collection(self,_collection, name):
    #     self.mongo.rename_collection(_collection,name)

    def write_drawdown_data(self, strategy_name, drawdown_abstract_df):
        """write drawdown_data database, plesase store abstract data rather than raw data """
        self.db = self.conn[self.drawdown_data]
        if strategy_name in self.db.list_collection_names():
            print(f"{strategy_name} collection already exist in drawdown_data")
        else:
            drawdown_abstract_records = drawdown_abstract_df.to_dict(orient='records')
            coll = self.db[strategy_name]
            coll.insert_many(drawdown_abstract_records)
        return

    def write_drawdown_graph_data(self, strategy_name, drawdown_raw_df):
        """write drawdown_graph_data database, please use raw data but not abstract data"""
        self.db = self.conn[self.drawdown_graph_data]
        if strategy_name in self.db.list_collection_names():
            print(f"{strategy_name} collection already exist in drawdown_graph_data")
        else:
            drawdown_raw_records = drawdown_raw_df.to_dict(orient='records')
            coll = self.db[strategy_name]
            coll.insert_many(drawdown_raw_records)
        return

    def write_simulation(self, strategy_name, run_df):
        """write simulation database"""
        self.db = self.conn[self.simulation]
        if strategy_name in self.db.list_collection_names():
            print(f"{strategy_name} collection already exist in simulaiton")
        else:
            run_records = run_df.to_dict(orient='records')
            coll = self.db[strategy_name]
            coll.insert_many(run_records)
        return

    def write_strategies(self, strategy_name, all_file_return_df):
        """write Strategies collection in rainydrop"""
        self.db = self.conn[self.rainydrop]
        coll = self.db[self.strategies]
        if coll.count_documents({'strategy_name':strategy_name}) > 0:
            print('document already exist in Strategies')
        else:
            all_file_return_record = all_file_return_df.to_dict(orient='records')
            coll.insert_many(all_file_return_record)
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

    def write_new_backtest_result(self, strategy_name, drawdown_abstract_df, drawdown_raw_df, run_df, all_file_return_df):
        """call whenever upload new backtest data, initiate everything"""
        self.write_drawdown_data(strategy_name, drawdown_abstract_df)
        self.write_drawdown_graph_data(strategy_name, drawdown_raw_df)
        self.write_simulation(strategy_name, run_df)
        self.write_strategies(strategy_name, all_file_return_df)
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