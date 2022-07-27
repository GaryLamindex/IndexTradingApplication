import json

from pymongo import MongoClient
import certifi
import requests
import pandas as pd
import datetime


class Write_Mongodb:
    nft_flask = 'nft-flask'
    watchlist_suggestions = 'watchlistSuggestions'
    trading_cards = 'tradingCards'
    strategyequity = 'strategyEquity'
    rollingReturns = 'rollingReturns'
    algoPrincipleTop = 'algoPrincipleTop'
    rainydrop = 'rainydrop'
    Strategies = 'Strategies'

    def __init__(self, run_data_df=None, all_file_return_df=None, drawdown_abstract_df=None, drawdown_raw_df=None):
        self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=certifi.where())
        print(f"Successful connection to mongoClient")
        self.rainydrop_db = self.conn[self.rainydrop]
        self.nft_db = self.conn[self.nft_flask]
        self.run_data_df = run_data_df
        self.all_file_return_df = all_file_return_df
        self.drawdown_abstract_df = drawdown_abstract_df
        self.drawdown_raw_df = drawdown_raw_df
        self.mongo = None

    def portfolio_efficiency(self):
        return

    def write_watchlist_suggestion(self, suggestion_id):
        coll = self.nft_db[self.watchlist_suggestions]
        if coll.count_documents({'suggestion_id': suggestion_id}) > 0:
            print('document already exist in transactions')
        else:
            suggestion_df = pd.DataFrame()
            volatility = self.all_file_return_df['Since Inception Volatility'].values.tolist()
            return_ratio = self.all_file_return_df['Since Inception Return'].values.tolist()
            name = list(self.all_file_return_df.index.values)
            suggestion_df['Volatility'] = volatility
            suggestion_df['Return'] = return_ratio
            suggestion_df['Name'] = name
            coll.insert_many(suggestion_df.to_dict(orient='records'))
        return

    def write_trading_cards(self, trading_cards_id, strategy_name, strategy_initial):
        coll = self.nft_db[self.trading_cards]
        if coll.count_documents({'trading_id': trading_cards_id}) > 0:
            print('document already exist in transactions')
        else:
            trading_cards_df = pd.DataFrame()
            daily_change = self.all_file_return_df['last daily change'].values.tolist()
            monthly_change = self.all_file_return_df['last monthly change'].values.tolist()
            trading_cards_df['strategy name'] = strategy_name
            trading_cards_df['nlvDailyChange'] = daily_change
            trading_cards_df['nlvMonthlyChange'] = monthly_change
            trading_cards_df['strategyInitial'] = strategy_initial
            coll.insert_many(trading_cards_df.to_dict(orient='records'))
        return

    # for reference
    def update_algo_principle_top(self):
        coll = self.rainydrop_db[self.Strategies]
        insert_coll = self.nft_db[self.algoPrincipleTop]

        # datetime, avg return, volatility, winrate ,annualreturn, sharpratio
        documents = coll.find({}, {'_id': 0,
                                   'strategy_name': 1,
                                   '1 Yr Volatility': 1,
                                   '1 Yr Win Rate': 1,
                                   '1 Yr Return': 1,
                                   '1 Yr Sharpe': 1,
                                   '1 Yr Average Win Per Day': 1})

        for x in documents:
            x['datetime'] = datetime.datetime.now()
            insert_coll.replace_one({"strategy_name": x['strategy_name']}, x, upsert=True)
            print("successfully updated:\n", x)

        return

def main():
    a = Write_Mongodb()
    # a.update_algo_principle_top()


if __name__== "__main__":
    main()