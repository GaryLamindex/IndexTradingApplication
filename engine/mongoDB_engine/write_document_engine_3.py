import json
from datetime import datetime
from pymongo import MongoClient
import certifi
import requests
import pandas as pd


class Write_Mongodb:
    """
    call the constructor of this class in order to connect to garylam mongoDB server
    call write_new_backtest_result() for uploading new backtest results
    """

    # for connection
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
    nft_flask = 'nft-flask'
    watchlist_suggestions = 'watchlistSuggestions'
    trading_cards = 'tradingCards'
    strategyequity = 'strategyEquity'
    stockinfoprinciple = 'stockinfoPrincipleTable'
    historicalgraph = 'historicalGraphNew'

    def __init__(self):
        self.conn = MongoClient(
            'mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',
            tlsCAFile=certifi.where())
        print(f"Successful connection to mongoClient")

    def write_watchlist_suggestion(self, suggestion_id, all_file_return_df):
        self.db = self.conn[self.nft_flask]
        coll = self.db[self.watchlist_suggestions]
        if coll.count_documents({'suggestion_id': suggestion_id}) > 0:
            print('document already exist in transactions')
        else:
            suggestion_df = pd.DataFrame()
            volatility = all_file_return_df['Since Inception Volatility'].values.tolist()
            return_ratio = all_file_return_df['Since Inception Return'].values.tolist()
            name = all_file_return_df.index.values.tolist()
            suggestion_df['Volatility'] = volatility
            suggestion_df['Return'] = return_ratio
            suggestion_df['Name'] = name
            suggestion = suggestion_df.to_dict(orient='records')
            coll.insert_many(suggestion)
        return

    def write_strategyEquity(self, equity_id):
        self.db = self.conn[self.nft_flask]
        coll = self.db[self.strategyequity]
        if coll.count_documents({'equity_id': equity_id}) > 0:
            print('document already exist in transactions')
        else:
            equity_dict = {"timestamp": [], "created at": []}
            equity_dict["created at"].append(datetime.now())

    def write_stockinfoPrincipleTable(self, stock_id, all_file_return_df):
        self.db = self.conn[self.nft_flask]
        coll = self.db[self.stockinfoprinciple]
        if coll.count_documents({'stock_id': stock_id}) > 0:
            print('document already exist in transactions')
        else:
            stock_df = pd.DataFrame()
            name = all_file_return_df.index.values.tolist()
            volatility = all_file_return_df['Since Inception Volatility'].values.tolist()
            stock_df['Volatility'] = volatility
            stock_df['Name'] = name
            stock = stock_df.dict(orient='records')
            coll.insert_many(stock)

    def write_rolling_returns(self, rolling_id, all_file_return_df):
        self.db = self.conn[self.nft_flask]
        coll = self.db[self.stockinfoprinciple]
        if coll.count_documents({'rolling_id': rolling_id}) > 0:
            print('document already exist in transactions')
        else:
            arr = [1, 2, 3, 5, 7, 10, 15, 20]
            for y in range(len(all_file_return_df)):
                for x in arr:
                    rolling_dict = {"period": [], "average_return": [], "best_return": [], "worst_return": [],
                                    "negative_periods": [], "created at": []}
                    rolling_dict["period"].append(f"{x} Year")
                    rolling_dict["created at"].append(datetime.now())
                    temp = all_file_return_df.loc[y, f"{x} Yr Rolling Return"]
                    rolling_dict['average_return'].append(temp["average_annual_return"])
                    rolling_dict['best_return'].append(temp["max_annual_rolling_return"])
                    rolling_dict['worst_return'].append(temp["min_annual_return"])
                    rolling_dict['negative_periods'].append(temp['negative_periods'])
                    coll.insert_one(rolling_dict)

    def write_trading_cards(self, trading_cards_id, strategy_name, strategy_initial, all_file_return_df):
        self.db = self.conn[self.nft_flask]
        coll = self.db[self.trading_cards]
        if coll.count_documents({'trading_id': trading_cards_id}) > 0:
            print('document already exist in transactions')
        else:
            trading_cards_df = pd.DataFrame()
            daily_change = all_file_return_df['last daily change'].values.tolist()
            monthly_change = all_file_return_df['last monthly change'].values.tolist()
            trading_cards_df['strategy name'] = strategy_name
            trading_cards_df['nlvDailyChange'] = daily_change
            trading_cards_df['nlvMonthlyChange'] = monthly_change
            trading_cards_df['strategyInitial'] = strategy_initial
            trading_cards = trading_cards_df.to_dict()
            coll.insert_many(trading_cards)
        return

    def historical_graph_new(self):
        self.db = self.conn[self.nft_flask]
        self.db2 = self.conn[self.simulation]
        trading_card_coll = self.db[self.trading_cards]

        documents = trading_card_coll.find({}, {'strategyName': 1})
        graph_dict = {"key": [], "trading_card_id": [], "data": []}
        for x in documents:
            x['_id'] = str(x['_id'])
            graph_dict['trading_card_id'].append(x['id'])
            graph_dict['key'].append(x['strategyName'])
        insert_coll = self.db[self.historicalgraph]
        insert_coll.insert_one(graph_dict)



    def write_new_backtest_result(self, all_file_return_df, strategy_initial, strategy_name):
        self.write_watchlist_suggestion(suggestion_id, all_file_return_df)
        self.write_trading_cards(trading_cards_id, strategy_name, strategy_initial, all_file_return_df)


def main():
    data = json.dumps({'ETF_percentage': 0.2391, 'ETF_label': 'HELLO'})
    requests.post('http://127.0.0.1:5000/composite/asset-allocation-etfs', json=data)
    return


if __name__ == "__main__":
    main()
