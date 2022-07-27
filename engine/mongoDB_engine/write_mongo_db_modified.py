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
    drawdown_data = 'drawdown_data'
    simulation = 'simulation'
    drawdown_graph_data = 'drawdown_graph_data'

    def __init__(self, run_data_df=None, all_file_return_df=None, drawdown_abstract_df=None, drawdown_raw_df=None):
        self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=certifi.where())
        print(f"Successful connection to mongoClient")
        self.rainydrop_db = self.conn[self.rainydrop]
        self.nft_db = self.conn[self.nft_flask]
        self.drawdown_data_db = self.conn[self.drawdown_data]
        self.simulation_db = self.conn[self.simulation]
        self.drawdown_graph_data_db = self.conn[self.drawdown_graph_data]
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

        documents = coll.find({}, {'_id': 0,
                                   'strategy_name': 1,
                                   '1 Yr Volatility': 1,
                                   '1 Yr Win Rate': 1,
                                   '1 Yr Return': 1,
                                   '1 Yr Sharpe': 1,
                                   '1 Yr Average Win Per Day': 1})

        for x in documents:
            r = {'strategy_name': 1,
                 'datetime': datetime.datetime.now(),
                 'avgReturn': x['1 Yr Average Win Per Day'],
                 'volatility': x['1 Yr Volatility'],
                 'winrate': x['1 Yr Win Rate'],
                 'annualReturn': x['1 Yr Win Rate'],
                 'sharpRatio': x['1 Yr Sharpe']
                 }

            insert_coll.replace_one({"strategy_name": x['strategy_name']}, r, upsert=True)
            print("successfully updated:\n", x)

        return

    def historical_graph(self):
        coll = self.simulation_db[self.simulation]

        # time stamp, price
        document = coll.find({}, {'timestamp': 1,
                                  'NetLiquidation': 1})
        




    def update_drawdown_data(self):
        insert_coll = self.nft_db[self.drawdown_data]
        for coll in self.drawdown_data_db.list_collection_names():
            return_document = dict()
            print(coll)
            documents = self.drawdown_data_db[coll].find({})
            for x in documents:
                return_document['strategy_name'] = coll
                return_document['drawdown'] = x['Drawdown']
                start_bottom_list = x['Drawdown period'].strip('][').replace('\'', "").split(', ')
                return_document['drawdown_start'] = start_bottom_list[0]
                return_document['drawdown_bottom'] = start_bottom_list[1]
                return_document['drawdown_days'] = x['Drawdown days']
                return_document['recovery_end'] = x['Recovery date']
                return_document['recovery_days'] = x['Recovery days']
                insert_coll.replace_one({'strategy_name': coll, '_id': x['_id']}, return_document, upsert=True)
                print(return_document)

        return

    def portfolio_returns(self):
        coll = self.rainydrop_db[self.Strategies]
        # insert_coll = self.nft_db[self.algoPrincipleTop]
        documents = coll.find({}, {'_id': 0,
                                   'strategy_name': 1,
                                   'YTD Return': 1,
                                   '1 Yr Return': 1,
                                   '3 Yr Return': 1,
                                   '5 Yr Return': 1,
                                   'Since Inception Return': 1
                                   })

        # for x in documents:
        #     insert_coll.replace_one({"strategy_name": x['strategy_name']}, x, upsert=True)
        #     print("successfully updated:\n", x)

        return

    def composite_table(self):
        coll = self.rainydrop_db[self.Strategies]
        # insert_coll = self.nft_db[self.algoPrincipleTop]
        documents = coll.find({}, {'_id': 0,
                                   'Composite': 1})

        for x in documents:
            sep_dict = x['Composite']
            print(sep_dict)

        return

    # not done
    def historical_returns(self):
        # 'period'
        # 'return'
        # 'adj_return'
        # 'standard_deviation'
        # 'max_drawdown'
        # 'pos_neg_months'
        coll = self.rainydrop_db[self.Strategies]
        # insert_coll = self.nft_db[self.algoPrincipleTop]
        documents = coll.find({}, {'_id': 0,
                                   'Composite': 1})


        return

    def algo_info_overview(self):

        coll = self.rainydrop_db[self.Strategies]
        # insert_coll = self.nft_db[self.algoPrincipleTop]
        documents = coll.find({}, {'_id': 0,
                                   'Since Inception Return': 1,
                                   'net profit': 1,
                                   'Since Inception Alpha': 1,
                                   'Since Inception Sharpe': 1,
                                   'compound_inception_return_dict': 1,
                                   'margin ratio': 1,
                                   'Since Inception Sortino': 1,
                                   'Since Inception Volatility': 1,
                                   'Since Inception Win Rate': 1,
                                   'Since Inception Max Drawdown': 1,
                                   'Since Inception Average Win Per Day': 1,
                                   'inception pos neg': 1})
        return

    def trade_log(self):
        # insert_coll = self.nft_db[self.drawdown_data]
        for coll in self.simulation_db.list_collection_names():
            print(coll)
            documents = self.simulation_db[coll].find({})
            for x in documents:
                print(x)
        return

    def drawdown(self):
        for coll in self.drawdown_graph_data_db.list_collection_names():
            print(coll)
            documents = self.drawdown_graph_data_db[coll].find({})
            for x in documents:
                print(x)
        return

    def rolling_return(self):
        for coll in self.rainydrop_db.list_collection_names():
            print(coll)
            documents = self.rainydrop_db[coll].find({},{'_id': 0,
                                                         '1 Yr Rolling Return': 1,
                                                         '2 Yr Rolling Return': 1,
                                                         '3 Yr Rolling Return': 1,
                                                         '5 Yr Rolling Return': 1,
                                                         '7 Yr Rolling Return': 1,
                                                         '10 Yr Rolling Return': 1,
                                                         '15 Yr Rolling Return': 1,
                                                         '20 Yr Rolling Return': 1,
                                                         })
            for x in documents:
                print(x)

def main():
    a = Write_Mongodb()
    # a.update_algo_principle_top()
    # a.update_drawdown_data()
    # a.composite_table()
    # a.trade_log()
    # a.drawdown()
    a.rolling_return()


if __name__== "__main__":
    main()