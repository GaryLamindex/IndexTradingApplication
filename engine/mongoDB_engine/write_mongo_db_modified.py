import json

from pymongo import MongoClient
import certifi
import requests
import pandas as pd
from datetime import datetime


class Write_Mongodb:
    nft_flask = 'nft-flask'
    watchlist_suggestions = 'watchlistSuggestions'
    trading_cards = 'tradingCards'
    trading_cards_new = 'tradingCardsNew'
    strategyequity = 'strategyEquity'
    rollingReturns = 'rollingReturns'
    rollingReturns_new = 'rollingReturns_new'
    algoPrincipleTop = 'algoPrincipleTop'
    rainydrop = 'rainydrop'
    Strategies = 'Strategies'
    drawdown_data = 'drawdown_data'
    simulation = 'simulation'
    drawdown_graph_data = 'drawdown_graph_data'
    historical_returns_new = 'HistoricalReturns_new'
    portfolio_efficiency_new = 'PortfolioEfficiency_new'

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

    # def write_trading_cards(self, trading_cards_id, strategy_name, strategy_initial):
    #     coll = self.nft_db[self.trading_cards]
    #     if coll.count_documents({'trading_id': trading_cards_id}) > 0:
    #         print('document already exist in transactions')
    #     else:
    #         trading_cards_df = pd.DataFrame()
    #         daily_change = self.all_file_return_df['last daily change'].values.tolist()
    #         monthly_change = self.all_file_return_df['last monthly change'].values.tolist()
    #         trading_cards_df['strategy name'] = strategy_name
    #         trading_cards_df['nlvDailyChange'] = daily_change
    #         trading_cards_df['nlvMonthlyChange'] = monthly_change
    #         trading_cards_df['strategyInitial'] = strategy_initial
    #         coll.insert_many(trading_cards_df.to_dict(orient='records'))
    #     return
    # for reference
    # def update_algo_principle_top(self):
    #     coll = self.rainydrop_db[self.Strategies]
    #     insert_coll = self.nft_db[self.algoPrincipleTop]
    #
    #     documents = coll.find({}, {'_id': 0,
    #                                'strategy_name': 1,
    #                                '1 Yr Volatility': 1,
    #                                '1 Yr Win Rate': 1,
    #                                '1 Yr Return': 1,
    #                                '1 Yr Sharpe': 1,
    #                                '1 Yr Average Win Per Day': 1})
    #
    #     for x in documents:
    #         r = {'strategy_name': x['strategy_name'],
    #              'datetime': datetime.datetime.now(),
    #              'avgReturn': x['1 Yr Average Win Per Day'],
    #              'volatility': x['1 Yr Volatility'],
    #              'winrate': x['1 Yr Win Rate'],
    #              'annualReturn': x['1 Yr Win Rate'],
    #              'sharpRatio': x['1 Yr Sharpe']
    #              }
    #
    #         insert_coll.replace_one({"strategy_name": x['strategy_name']}, r, upsert=True)
    #         print("successfully updated:\n", x)
    #
    #     return

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

    # def portfolio_returns(self):
    #     coll = self.rainydrop_db[self.Strategies]
    #     # insert_coll = self.nft_db[self.algoPrincipleTop]
    #     documents = coll.find({}, {'_id': 0,
    #                                'strategy_name': 1,
    #                                'YTD Return': 1,
    #                                '1 Yr Return': 1,
    #                                '3 Yr Return': 1,
    #                                '5 Yr Return': 1,
    #                                'Since Inception Return': 1
    #                                })
    #
    #     # for x in documents:
    #     #     insert_coll.replace_one({"strategy_name": x['strategy_name']}, x, upsert=True)
    #     #     print("successfully updated:\n", x)
    #
    #     return

    # def composite_table(self):
    #     coll = self.rainydrop_db[self.Strategies]
    #     # insert_coll = self.nft_db[self.algoPrincipleTop]
    #     documents = coll.find({}, {'_id': 0,
    #                                'strategy_name': 1,
    #                                'Composite': 1})
    #
    #     for x in documents:
    #         sep_dict = x['Composite']
    #         print(sep_dict)
    #
    #     return


    # def trade_log(self):
    #     # insert_coll = self.nft_db[self.drawdown_data]
    #     for coll in self.simulation_db.list_collection_names():
    #         print(coll)
    #         documents = self.simulation_db[coll].find({})
    #         for x in documents:
    #             print(x)
    #     return

    def update_drawdown_graph_data(self):
        trading_card_new_coll = self.nft_db[self.trading_cards_new]
        drawdown_graph_data_coll = self.nft_db[self.drawdown_graph_data]
        for coll in self.drawdown_graph_data_db.list_collection_names():
            print(coll)
            t_dict = trading_card_new_coll.find_one({'strategyName': coll}, {'_id': 1})
            trading_card_id = str(t_dict['_id']) if t_dict is not None else None
            documents = self.drawdown_graph_data_db[coll].find({})
            print(trading_card_id)
            for x in documents:
                x['trading_card_id'] = trading_card_id
                x.pop('_id')
                x['x'] = datetime.fromtimestamp(x['timestamp'])
                x['y'] = x.pop('drawdown')
                x['created_at'] = datetime.now()
                print(x)
                drawdown_graph_data_coll.replace_one({'timestamp': x['timestamp'],
                                                      'trading_card_id': x['trading_card_id']}, x, upsert=True)
        return

    # def rolling_return(self):
    #     coll = self.nft_db[self.trading_cards]
    #     replace = self.nft_db[self.rollingReturns_new]
    #     documents = self.rainydrop_db[self.Strategies].find({}, {'_id': 0,
    #                                                             '1 Yr Rolling Return': 1,
    #                                                              '2 Yr Rolling Return': 1,
    #                                                              '3 Yr Rolling Return': 1,
    #                                                              '5 Yr Rolling Return': 1,
    #                                                              '7 Yr Rolling Return': 1,
    #                                                              '10 Yr Rolling Return': 1,
    #                                                              '15 Yr Rolling Return': 1,
    #                                                              '20 Yr Rolling Return': 1,
    #                                                              'strategy_name': 1
    #                                                              })
    #     for x in documents:

    #         trading_card_id = coll.find({'strategyName': x['strategy_name']}, {'_id': 1})

    #         del x['strategy_name']
    #         for y in trading_card_id:
    #             y['_id'] = str(y['_id'])

    #         try:
    #             for key, value in x.items():
    #                 dict_copy = {'period': key,
    #                             'average_return': value['average_annual_return'],
    #                              'best_return': str(value['max_annual_rolling_return']) + " (" + str(
    #                                 value['dateinfo_index_max']) + ")",
    #                              'worst_return': str(value['min_annual_rolling_return']) + " (" + str(
    #                                  value['dateinfo_index_min']) + ")",
    #                              'negative_periods': str(value['negative_periods']),
    #                              'trading_card_id': y['_id']}
    #                 replace.replace_one(
    #                     {'period': dict_copy['period'], 'trading_card_id': dict_copy['trading_card_id']}, dict_copy,
    #                     upsert=True)
    #                 # print(dict_copy)
    #        except:
    #            continue

    def write_trading_card(self):
        coll = self.nft_db[self.trading_cards_new]
        for coll_name in self.simulation_db.list_collection_names():
            print(coll_name)
            if coll.count_documents({'strategyName': coll_name}) > 0:
                print('document already exist in TRADE CARD')
            else:

                nlv_change = self.rainydrop_db[self.Strategies].find_one({'strategy_name': coll_name},
                                                               {'_id': 0,'strategy_name': 1,'last daily change': 1,
                                                                'last monthly change': 1, 'strategy_initial': 1,
                                                                'Since Inception Max Drawdown': 1,
                                                                'Since Inception Profit Loss Ratio': 1,
                                                                'Since Inception Return': 1,
                                                                'Since Inception Sharpe': 1,
                                                                'Since Inception Win Rate': 1})
                if nlv_change is not None:
                    if nlv_change['strategy_initial'] is None:
                        nlv_change['strategy_initial'] = "None"
                    return_dict = {'strategyName': nlv_change['strategy_name'],
                                   'nlvDailyChange': nlv_change['last daily change'],
                                   'nlvMonthlyChange': nlv_change['last monthly change'],
                                   'strategyInitial': nlv_change['strategy_initial'],
                                   'maxDrawdown': nlv_change['Since Inception Max Drawdown'],
                                   'profitLossRatio': nlv_change['Since Inception Profit Loss Ratio'],
                                   'returnPercentage': nlv_change['Since Inception Return'],
                                   'sharpeRatio': nlv_change['Since Inception Sharpe'],
                                   'winRate': nlv_change['Since Inception Win Rate']}
                    print(nlv_change)
                    coll.replace_one({'strategyName': return_dict['strategyName']}, return_dict, upsert=True)
            # for x in documents:
            #     print(x)
        return

    def update_historical_return_new(self):
        coll = self.rainydrop_db[self.Strategies]
        trading_card_coll = self.nft_db[self.trading_cards_new]
        historical_returns_new_coll = self.nft_db[self.historical_returns_new]
        x = coll.find({},{'_id':0,
                      'YTD Return':1,
                      '1 Yr Return':1,
                      '3 Yr Return':1,
                      '5 Yr Return':1,
                      'Since Inception Return':1,
                      '1 yr sd':1,
                      '3 yr sd':1,
                      '5 yr sd':1,
                      'inception sd':1,
                      '1 yr pos neg': 1,
                      '3 yr pos neg': 1,
                      '5 yr pos neg': 1,
                      'inception pos neg': 1,
                      '1 Yr Max Drawdown': 1,
                      '3 Yr Max Drawdown': 1,
                      '5 Yr Max Drawdown': 1,
                      'YTD Max Drawdown': 1,
                      'Since Inception Max Drawdown': 1,
                      'strategy_name':1
                      })
        for document in x:

            document['YTD Return'] = document.get('YTD Return', 0)
            document['1 Yr Return'] = document.get('1 Yr Return', 0)
            document['3 Yr Return'] = document.get('3 Yr Return', 0)
            document['5 Yr Return'] = document.get('5 Yr Return', 0)
            document['Since Inception Return'] = document.get('Since Inception Return', 0)
            document['1 yr sd'] = document.get('1 yr sd', 0)
            document['3 yr sd'] = document.get('3 yr sd', 0)
            document['5 yr sd'] = document.get('5 yr sd', 0)
            document['inception sd'] = document.get('inception sd', 0)
            document['1 yr pos neg'] = document.get('1 yr pos neg', 0)
            document['3 yr pos neg'] = document.get('3 yr pos neg', 0)
            document['5 yr pos neg'] = document.get('5 yr pos neg', 0)
            document['inception pos neg'] = document.get('inception pos neg', 0)
            document['1 Yr Max Drawdown'] = document.get('1 Yr Max Drawdown', 0)
            document['3 Yr Max Drawdown'] = document.get('3 Yr Max Drawdown', 0)
            document['5 Yr Max Drawdown'] = document.get('5 Yr Max Drawdown', 0)
            document['YTD Max Drawdown'] = document.get('YTD Max Drawdown', 0)
            document['Since Inception Max Drawdown'] = document.get('Since Inception Max Drawdown', 0)
            document['strategy_name'] = document.get('strategy_name', 0)

            trading_card = trading_card_coll.find({'strategyName': document['strategy_name']},{'_id':1})
            for y in trading_card:
                y['_id'] = str(y['_id'])
                document['trading_card_id'] = y['_id']
                one_yr_dict = {'period': '1Y',
                               'return': str(document['1 Yr Return']),
                               'adj_return': str(document['1 Yr Return']),
                               'standard_deviation': str(document['1 yr sd']),
                               'max_drawdown': str(document['1 Yr Max Drawdown']),
                               'pos_neg_months': str(document['1 yr pos neg']),
                               'trading_card_id': str(document['trading_card_id'])}
                three_yr_dict = {'period': '3Y',
                               'return': str(document['3 Yr Return']),
                               'adj_return': str(document['3 Yr Return']),
                               'standard_deviation': str(document['3 yr sd']),
                               'max_drawdown': str(document['3 Yr Max Drawdown']),
                               'pos_neg_months': str(document['3 yr pos neg']),
                               'trading_card_id': str(document['trading_card_id'])}
                five_yr_dict = {'period': '5Y',
                               'return': str(document['5 Yr Return']),
                               'adj_return': str(document['5 Yr Return']),
                               'standard_deviation': str(document['5 yr sd']),
                               'max_drawdown': str(document['5 Yr Max Drawdown']),
                               'pos_neg_months': str(document['5 yr pos neg']),
                               'trading_card_id': str(document['trading_card_id'])}
                YTD_dict = {'period': 'YTD',
                               'return': str(document['YTD Return']),
                               'adj_return': str(document['YTD Return']),
                               'standard_deviation': 'No data',
                               'max_drawdown': str(document['YTD Max Drawdown']),
                               'pos_neg_months': 'No data',
                               'trading_card_id': str(document['trading_card_id'])}
                inception_dict = {'period': 'MAX','return': str(document['Since Inception Return']),
                                  'adj_return': str(document['Since Inception Return']),
                                  'standard_deviation': str(document['inception sd']),
                                  'max_drawdown': str(document['Since Inception Max Drawdown']),
                                  'pos_neg_months': str(document['inception pos neg']),
                                  'trading_card_id': str(document['trading_card_id'])}

                historical_returns_new_coll.replace_one(
                    {'period': '1Y', 'trading_card_id': str(document['trading_card_id'])},
                    one_yr_dict, upsert=True
                )
                historical_returns_new_coll.replace_one(
                    {'period': '3Y', 'trading_card_id': str(document['trading_card_id'])},
                    three_yr_dict, upsert=True
                )
                historical_returns_new_coll.replace_one(
                    {'period': '5Y', 'trading_card_id': str(document['trading_card_id'])},
                    five_yr_dict, upsert=True
                )
                historical_returns_new_coll.replace_one(
                    {'period': 'YTD', 'trading_card_id': str(document['trading_card_id'])},
                    YTD_dict, upsert=True
                )
                historical_returns_new_coll.replace_one(
                    {'period': 'MAX', 'trading_card_id': str(document['trading_card_id'])},
                    inception_dict, upsert=True
                )
        return

    def update_portfolio_efficiency_new(self):
        coll = self.nft_db[self.portfolio_efficiency_new]
        return_dict = dict()
        trading_card_coll = self.nft_db[self.trading_cards_new]
        for name in self.simulation_db.list_collection_names():
            x = trading_card_coll.find({'strategyName': name}, {'_id':1})
            for item in x:
                return_dict['trading_card_id'] = str(item['_id'])
                return_dict['parameter'] = 'None'
                return_dict['value'] = 'None'
                return_dict['compare_same_risk'] = 'None'
                return_dict['compare_all_portfolios'] = 'None'

                print(return_dict)

                coll.replace_one({'trading_card_id':return_dict['trading_card_id']}, return_dict, upsert=True)
        return

    def write_historical_graph_new(self):
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
            insert_coll.replace_one({'key': graph_dict['key']}, graph_dict, upsert=True)

    # def algo_info_overview(self):
    #     self.db = self.conn['rainydrop']
    #     coll = self.db['Strategies']
    #     self.db2 = self.conn["nft-flask"]
    #     trading_card_coll = self.db2['tradingCardsNew']
    #     doc = trading_card_coll.find({}, {'strategyName': 1})
    #     for y in doc:
    #         try:
    #             y['_id'] = str(y['_id'])
    #             documents = coll.find({'strategy_name': y['strategyName']}, {'_id': 0,
    #                                                                          'Since Inception Return': 1,
    #                                                                          'net profit': 1,
    #                                                                          'Since Inception Alpha': 1,
    #                                                                          'Since Inception Sharpe': 1,
    #                                                                          'compound_inception_return_dict': 1,
    #                                                                          'margin ratio': 1,
    #                                                                          'Since Inception Sortino': 1,
    #                                                                          'Since Inception Volatility': 1,
    #                                                                          'Since Inception Win Rate': 1,
    #                                                                          'Since Inception Max Drawdown': 1,
    #                                                                          'Since Inception Average Win Per Day': 1,
    #                                                                          'inception pos neg': 1,
    #                                                                          'Since Inception Profit Loss Ratio': 1,
    #                                                                          'strategy_name': 1})
    #             for x in documents:
    #                 algo_dict = {}
    #                 algo_dict['total_return_percentage'] = x['Since Inception Return']
    #                 algo_dict['net_profit'] = x['net profit']
    #                 algo_dict['sharpe_ratio'] = x['Since Inception Sharpe']
    #                 algo_dict['compounding_return'] = x['compound_inception_return_dict']
    #                 algo_dict['margin_ratio'] = x['margin ratio']
    #                 algo_dict['sortino_ratio'] = x['Since Inception Sortino']
    #                 algo_dict['max_drawdown'] = x['Since Inception Max Drawdown']
    #                 algo_dict['alpha'] = x['Since Inception Alpha']
    #                 algo_dict['volatility'] = x['Since Inception Volatility']
    #                 algo_dict['profit_loss_ratio'] = x['Since Inception Profit Loss Ratio']
    #                 algo_dict['win_rate'] = x['Since Inception Win Rate']
    #                 algo_dict['average_win'] = x['Since Inception Average Win Per Day']
    #                 algo_dict['trading_card_id'] = y['_id']
    #                 insert_coll = self.db2['algoInfoOverview_new']
    #                 insert_coll.replace_one({'trading_card_id': algo_dict['trading_card_id'],
    #                                          'total_return_percentage': algo_dict['total_return_percentage'],
    #                                          'net_profit': algo_dict['net_profit'],
    #                                          'sharpe_ratio': algo_dict['sharpe_ratio'],
    #                                          'compounding_return': algo_dict['compounding_return'],
    #                                          'average_win': algo_dict['average_win']}, algo_dict, upsert=True)
    #                 print(algo_dict)
    #         except:
    #             continue

    def trade_log_new(self):
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
                    trade_dict['price'] = price[z]
                    trade_dict['quantity'] = quantity[z]
                    trade_dict['proceeds'] = proceeds[z]
                    trade_dict['trading_card_id'] = x['_id']
                    insert_coll = self.db['TradeLog_new']
                    insert_coll.replace_one({'trading_card_id': trade_dict['trading_card_id'],
                                             'ETF_Name': trade_dict['ETF_Name'],
                                             'price': trade_dict['price'],
                                             'quantity': trade_dict['quantity'],
                                             'proceeds': trade_dict['proceeds']}, trade_dict, upsert=True)
                    print(trade_dict)

    def run_all(self):
        self.write_trading_card()
        self.trade_log_new()
        self.write_historical_graph_new()
        self.update_portfolio_efficiency_new()
        self.update_historical_return_new()
        self.update_drawdown_graph_data()
        self.update_drawdown_data()




def main():
    a = Write_Mongodb()
    # a.update_algo_principle_top()
    # a.update_drawdown_data()
    # a.composite_table()
    # a.trade_log()
    # a.update_drawdown_graph_data()
    # a.rolling_return()
    # a.update_historical_return_new()
    # a.write_trading_card()
    # a.update_portfolio_efficiency_new()


if __name__== "__main__":
    main()