
from pymongo import MongoClient
import certifi



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

    def write_new_backtest_result(self, strategy_name, run_df):
        """call whenever upload new backtest data, initiate everything"""
        self.write_simulation(strategy_name, run_df)
        return

def main():
    pass



if __name__ == "__main__":
    main()