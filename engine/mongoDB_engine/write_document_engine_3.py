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
            name = list(all_file_return_df.index.values)
            suggestion_df['Volatility'] = volatility
            suggestion_df['Return'] = return_ratio
            suggestion_df['Name'] = name
        return

    def write_strategyEquity(self, equity_id):
        self.db = self.conn[self.nft_flask]
        coll = self.db[self.strategyequity]
        if coll.count_documents({'equity_id': equity_id}) > 0:
            print('document already exist in transactions')
        else:

    def write_trading_cards(self,trading_cards_id, strategy_name, strategy_initial, all_file_return_df):
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
        return

    def write_new_backtest_result(self, all_file_return_df, strategy_initial, strategy_name):
        self.write_watchlist_suggestion(suggestion_id, all_file_return_df)
        self.write_trading_cards(trading_cards_id, strategy_name, strategy_initial, all_file_return_df)


def main():
    data = json.dumps({'ETF_percentage': 0.2391, 'ETF_label': 'HELLO'})
    requests.post('http://127.0.0.1:5000/composite/asset-allocation-etfs', json=data)
    return


if __name__ == "__main__":
    main()
