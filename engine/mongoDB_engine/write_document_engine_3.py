import json
from datetime import datetime
from pymongo import MongoClient
import certifi
import requests
import pandas as pd
from selenium.common import NoSuchElementException


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
    trading_cards = 'tradingCardsNew'
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
        self.db = self.conn['nft-flask']
        self.db2 = self.conn["simulation"]
        trading_card_coll = self.db['tradingCardsNew']

        documents = trading_card_coll.find({}, {'strategyName': 1})
        for x in documents:
            graph_dict = {}
            x['_id'] = str(x['_id'])
            graph_dict['key'] = x['strategyName']
            graph_dict['trading_card_id'] = x['_id']
            data = self.db2[x['strategyName']].find({}, {'timestamp': 1, 'NetLiquidation': 1})
            array = list()
            for y in data:
                temp = [y['timestamp'], y['NetLiquidation']]
                array.append(temp)
            graph_dict['data'] = array
            insert_coll = self.db['historicalGraphNew']
            insert_coll.replace_one({'trading_card_id': graph_dict['trading_card_id'] }, graph_dict, upsert=True)
            print(graph_dict)

    def algo_info_overview(self):
        self.db = self.conn['rainydrop']
        coll = self.db['Strategies']
        self.db2 = self.conn["nft-flask"]
        trading_card_coll = self.db2['tradingCardsNew']
        doc = trading_card_coll.find({}, {'strategyName': 1})
        for y in doc:
            try:
                y['_id'] = str(y['_id'])
                documents = coll.find({'strategy_name': y['strategyName']}, {'_id': 0,
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
                                                                             'inception pos neg': 1,
                                                                             'Since Inception Profit Loss Ratio': 1,
                                                                             'strategy_name': 1})
                for x in documents:
                    algo_dict = {}
                    algo_dict['total_return_percentage'] = x['Since Inception Return']
                    algo_dict['net_profit'] = x['net profit']
                    algo_dict['sharpe_ratio'] = x['Since Inception Sharpe']
                    algo_dict['compounding_return'] = x['compound_inception_return_dict']
                    algo_dict['margin_ratio'] = x['margin ratio']
                    algo_dict['sortino_ratio'] = x['Since Inception Sortino']
                    algo_dict['max_drawdown'] = x['Since Inception Max Drawdown']
                    algo_dict['alpha'] = x['Since Inception Alpha']
                    algo_dict['volatility'] = x['Since Inception Volatility']
                    algo_dict['profit_loss_ratio'] = x['Since Inception Profit Loss Ratio']
                    algo_dict['win_rate'] = x['Since Inception Win Rate']
                    algo_dict['average_win'] = x['Since Inception Average Win Per Day']
                    algo_dict['trading_card_id'] = y['_id']
                    insert_coll = self.db2['algoInfoOverview_new']
                    insert_coll.replace_one({'trading_card_id': algo_dict['trading_card_id'] }, algo_dict, upsert=True)
                    print(algo_dict)
            except:
                continue

    def trade_log(self):
        self.db = self.conn['nft-flask']
        self.db2 = self.conn["simulation"]
        trading_card_coll = self.db['tradingCardsNew']
        documents = trading_card_coll.find({}, {'strategyName': 1})
        for x in documents:
            trade_dict = {}
            x['_id'] = str(x['_id'])
            col = self.db2[x['strategyName']]
            for y in col.find().sort('timestamp').limit(10):
                price = [v for k, v in y.items() if k.startswith('avgPrice') and k != 'avgPrice_']
                ticker_name = [k for k, v in y.items() if k.startswith('avgPrice')]
                ticker_name = [x.split('_')[1] for x in ticker_name]
                ticker_name = list(filter(None, ticker_name))
                quantity = [v for k, v in y.items() if k.startswith('totalQuantity') and k != 'totalQuantity_']
                proceeds = [v for k, v in y.items() if k.startswith('commission') and k != 'commission_']
                if not proceeds:
                    proceeds = [" "] * len(quantity)
                for z in range(len(price)):
                    trade_dict['ETF_Name'] = ticker_name[z]
                    trade_dict['date_time'] = datetime.now()
                    trade_dict['timestamp'] = y['timestamp']
                    trade_dict['price'] = price[z]
                    trade_dict['quantity'] = quantity[z]
                    trade_dict['proceeds'] = proceeds[z]
                    trade_dict['trading_card_id'] = x['_id']
                    insert_coll = self.db['TradeLog_new']
                    insert_coll.replace_one({'trading_card_id': trade_dict['trading_card_id'],
                                             'timestamp': trade_dict['timestamp']
                                             }, trade_dict, upsert=True)
                    print(trade_dict)

    def delete(self):
        self.db = self.conn['nft-flask']
        trading_card_coll = self.db['tradingCardsNew']
        documents = trading_card_coll.find({}, {'strategyName': 1})
        for x in documents:
            x['_id'] = str(x['_id'])
            col = self.db['algoInfoOverview_new']
            while col.count_documents({'trading_card_id': x['_id']}) != 0:
                col.delete_one({'trading_card_id': x['_id']})
    def delete_all(self):
        self.db = self.conn['rainydrop']
        col = self.db['Strategies']
        col.delete_many({})
    # def write_new_backtest_result(self, all_file_return_df, strategy_initial, strategy_name):
    #     self.write_watchlist_suggestion(suggestion_id, all_file_return_df)
    #     self.write_trading_cards(trading_cards_id, strategy_name, strategy_initial, all_file_return_df)


def main():
    # data = json.dumps({'ETF_percentage': 0.2391, 'ETF_label': 'HELLO'})
    # requests.post('http://127.0.0.1:5000/composite/asset-allocation-etfs', json=data)
    engine = Write_Mongodb()
    # engine.historical_graph_new()
    engine.algo_info_overview()
    # engine.trade_log()
    # engine.delete()
    # engine.delete_all()
    return


if __name__ == "__main__":
    main()
