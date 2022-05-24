# import math
# from datetime import datetime
#
# from pythonProject.engine.backtest_engine.trade_engine import backtest_trade_engine
# from pythonProject.engine.backtest_engine.stock_data_io_engine import local_engine
# from pythonProject.engine.backtest_engine.portfolio_data_engine import backtest_portfolio_data_engine
#
# from pythonProject.engine.simulation_engine.simulation_agent import simulation_agent
# from pythonProject.object.backtest_acc_data import backtest_acc_data
# from pythonProject.object.ibkr_acc_data import ibkr_acc_data
#
#
# class rebalance_margin_wif_max_drawdown_control(object):
#
#     tickers = []
#
#     dynamo_db = None
#     trade_agent = None
#     stock_data_io_engine = None
#     sim_agent = None
#     data_calculation_engine = None
#     acc_data = None
#
#     rebalance_margin = 0
#     maintain_margin = 0
#     data_engine = None
#
#     table_name = ""
#
#     max_drawdown_ratio = 0
#     purchase_exliq = 0
#     max_stock_price = {}
#     max_drawdown_price = {}
#
#     liq_sold_price_dict = {}
#     liq_sold_price_qty_dict ={}
#     max_drawdown_dodge = False
#
#     def __init__(self, tickers, table_info, table_name, spec, spec_str):
#
#         if table_info.get("mode") == "backtest":
#             self.stock_data_io_engine = local_engine(tickers, "one_min")
#             self.acc_data = backtest_acc_data(table_info.get("user_id"), table_info.get("strategy_name"), table_name, spec_str)
#             self.portfolio_data_engine = backtest_portfolio_data_engine(self.acc_data)
#             self.trade_agent = trade_agent(self.acc_data, self.stock_data_io_engine, self.portfolio_data_engine)
#             self.table_name = table_name
#         elif table_info.get("mode") == "real_time":
#             self.acc_data = ibkr_acc_data(table_info.get("user_id"), table_info.get("strategy_name"))
#             # self.trade_agent = ibkr_trade_agent(self.acc_data)
#             # self.stock_data_io_engine = ibkr_stock_data_agent(self.table_name)
#             # self.portfolio_data_agent = ibkr_portfolio_data_agent(self.table_name)
#
#
#         self.tickers = tickers
#         self.sim_agent = simulation_agent(spec, spec_str, table_info, False, self.acc_data)
#
#         self.rebalance_margin = spec.get("rebalance_margin")
#         self.maintain_margin = spec.get("maintain_margin")
#
#         self.max_drawdown_ratio = spec.get("max_drawdown_ratio")
#         self.purchase_exliq = spec.get("purchase_exliq")
#
#         for ticker in self.tickers:
#             self.max_stock_price.update({ticker: 0})
#             self.max_drawdown_price.update({ticker: 0})
#
#     def exec(self, stock_data_dict, sim_metadata):
#         action_msgs  = []
#         _his_sim_data_list = {}
#         timestamp = sim_metadata.get("timestamp")
#
#         #init orig_db_dict
#
#         if len(self.acc_data.portfolio)==0:
#
#             self.price_drawdown_calculation(stock_data_dict)
#             orig_acct_snapshot_dict = self.get_acct_snapshot()
#             orig_additional_stats_dict = self._cal_additional_stats_dict(orig_acct_snapshot_dict)
#             orig_db_snapshot_dict = orig_acct_snapshot_dict | orig_additional_stats_dict
#
#             #portfolio initialize strategy
#
#             for ticker in self.tickers:
#                 # calulate the trigerring point of purchase action of each ticker
#                 # purchase/sell ticker
#                 ticker_price = stock_data_dict.get(ticker)
#                 # calulate the trigerring point of purchase action of each ticker
#                 _ticker_quan = len(stock_data_dict)
#                 ticker_purchase_amount = orig_db_snapshot_dict.get("TotalCashValue") / _ticker_quan
#
#                 share_purchase = math.floor(ticker_purchase_amount / ticker_price)
#                 action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker, share_purchase, sim_metadata)
#                 action_msgs.append(action_msg)
#
#         else:
#
#             #calculate each day data
#             self.portfolio_data_engine.update_stock_price_and_portfolio_data(stock_data_dict)
#             # self.stock_data_io_engine.update_portfolio_dividend(div_data_dict)
#             self.price_drawdown_calculation(stock_data_dict)
#
#             #get snapshot after dividend and stockdata has changed the portfolio
#             orig_db_snapshot_dict = self.get_acct_snapshot()
#             orig_additional_stats_dict = self._cal_additional_stats_dict(orig_db_snapshot_dict)
#             orig_db_snapshot_dict = orig_db_snapshot_dict| orig_additional_stats_dict
#
#             # routine strategy starts here
#             if self.max_drawdown_dodge == True:
#                 # calulate max drawdonw and determine whether buy back
#                 for ticker in self.tickers:
#                     db_snapshot_dict = self.get_acct_snapshot()
#                     orig_buy_pwr = db_snapshot_dict.get("BuyingPower")
#                     ticker_price = stock_data_dict.get(ticker)
#
#
#                     self.check_if_shifting_max_drawdown_price(ticker, ticker_price)
#                     if self.check_buy_back(ticker, ticker_price):
#                         action_msg = self.buy_back_pos(ticker, ticker_price, orig_buy_pwr, sim_metadata)
#                         action_msgs.append(action_msg)
#                         self.max_drawdown_dodge = False
#             else:
#                 # calulate max drawdonw and determine whether dodge or not
#                 if (self.check_max_drawdown_dodge(stock_data_dict) == True):
#                     liq_action_msgs = self.liquidate_all_pos(stock_data_dict, timestamp)
#                     action_msgs += liq_action_msgs
#                     self.max_drawdown_dodge = True
#                 else:
#                     target_ex_liq = self.rebalance_margin * self.acc_data.mkt_value.get('GrossPositionValue')
#                     main_ex_liq = self.maintain_margin * self.acc_data.mkt_value.get('GrossPositionValue')
#
#                     if(orig_db_snapshot_dict.get("ExcessLiquidity") > target_ex_liq):
#                         print("orig_db_snapshot_dict.get(ExcessLiquidity)", orig_db_snapshot_dict.get("ExcessLiquidity"), "; target_ex_liq:", target_ex_liq)
#                         target_ex_liq = self.rebalance_margin * orig_db_snapshot_dict.get("GrossPositionValue")
#                         ex_liq_diff = orig_db_snapshot_dict.get("ExcessLiquidity") - target_ex_liq
#                         target_purchase_amount = ex_liq_diff
#                         target_purchase_each = target_purchase_amount * self.purchase_exliq
#                         print("target_purchase_amount:", target_purchase_amount, "; target_purchase_each:", target_purchase_each)
#                         for ticker in self.tickers:
#                             # calulate the trigerring point of purchase action of each ticker
#                             # purchase/sell ticker
#
#                             ticker_price = stock_data_dict.get(ticker)
#                             target_share_purchase = math.floor(target_purchase_each / ticker_price)
#
#                             print("ticker:", ticker, "; target_share_purchase:", target_share_purchase)
#                             if (target_share_purchase > 0):
#
#                                 action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker, target_share_purchase, sim_metadata)
#                                 action_msgs.append(action_msg)
#                             else:
#                                 continue
#                     elif(orig_db_snapshot_dict.get("ExcessLiquidity") < main_ex_liq):
#
#                         ex_liq_diff = main_ex_liq - orig_db_snapshot_dict.get("ExcessLiquidity")
#                         target_sell_amount = ex_liq_diff
#                         target_sell_each = target_sell_amount
#                         for ticker in self.tickers:
#                             ticker_price = stock_data_dict.get(ticker)
#                             target_share_sell = math.ceil(target_sell_each / ticker_price)
#                             if (target_share_sell > 0):
#                                 backtest_data = {"timestamp":timestamp}
#                                 action_msg = self.trade_agent.place_sell_stock_mkt_order(ticker,target_share_sell, backtest_data)
#                                 action_msgs.append(action_msg)
#                             else:
#                                 continue
#
#                     # strategy end here
#
#         #print each day end portfolio after action is taken
#         updated_db_snapshot_dict = self.get_acct_snapshot()
#         updated_additional_stats_dict = self._cal_additional_stats_dict(updated_db_snapshot_dict)
#         updated_db_snapshot_dict = updated_db_snapshot_dict|updated_additional_stats_dict
#         self.sim_agent.append_sim_data_to_db(timestamp, stock_data_dict, orig_db_snapshot_dict, updated_db_snapshot_dict,
#                                           action_msgs)
#         self.acc_data.write_acc_data()
#
#         pass
#
#     # def real_time(self):
#     #
#     #     #time looping per 60 sec
#     #     while True:
#     #
#     #         stock_data_dict = {}
#     #         # connect to ib to get price data
#     #         stock_data_agent = ibkr_stock_data_agent('rebalance_margin_wif_max_drawdown_control')
#     #         for ticker in self.tickers:
#     #             _real_time_data = stock_data_agent.query_real_time_data()
#     #             stock_data_dict.update({ticker: _real_time_data.get("bid_price")})
#     #             stock_data_dict.update({"timestamp": _real_time_data.get("last_timestamp")})
#     #
#     #         timestamp = stock_data_dict.get("timestamp")
#     #         _date = datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
#     #         _time = datetime.fromtimestamp(int(timestamp)).strftime("%H:%M:%S")
#     #         print('#' * 20, _date, ":", _time, '#' * 20)
#     #
#     #         # input database and historical data into algo
#     #         sim_medtadata = {"timestamp": timestamp}
#     #         self.exec(stock_data_dict, sim_medtadata)
#     #
#     #
#     #         sleep(60 - time() % 60)
#     #     #feed data to algo
#     #
#     #
#
#
#     def backtest(self, start_timestamp, end_timestamp, initial_amount):
#         # connect to downloaded ib data to get price data
#         print("start backtest")
#         row = 0
#         print("Fetch data")
#         stock_data_dict = {}
#         timestamps = self.stock_data_io_engine.get_data_by_range([start_timestamp, end_timestamp])['timestamp']
#
#         for timestamp in timestamps:
#             print("timestamp:", timestamp)
#             stock_data_dict = {}
#             stock_data_dict.update({"timestamp": timestamp})
#             for ticker in self.tickers:
#                 # get stock data from historical data
#                 print("timestamp:",timestamp,"; ticker:",ticker)
#                 ticker_data = self.stock_data_io_engine.get_ticker_item_by_timestamp(timestamp)
#                 ticker_open_price = ticker_data.get("open")
#                 print("ticker_open_price",ticker_open_price)
#                 stock_data_dict.update({ticker: ticker_open_price})
#                 print("stock_data_dict", stock_data_dict)
#
#                 # ticker_div = ticker_stock_data.get(Query().timestamp == timestamp).get(ticker + ' div amount')
#                 # div_data_dict.update({ticker + ' div amount': ticker_div})
#
#
#             _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
#             _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
#             print('#' * 20, _date, ":", _time, '#' * 20)
#
#             if row == 0:
#                 # input initial cash
#                 self.portfolio_data_engine.deposit_cash(initial_amount, timestamp)
#                 row += 1
#
#             # input database and historical data into algo
#             sim_metadata = {"timestamp": timestamp}
#             self.exec(stock_data_dict, sim_metadata)
#
#     def _cal_additional_stats_dict(self, updated_db_snapshot_dict):
#         _additional_stats_dict = {}
#         #cal exliq/mktvalue
#         ExcessLiquidity = updated_db_snapshot_dict.get("ExcessLiquidity")
#         GrossPositionValue = updated_db_snapshot_dict.get("GrossPositionValue")
#         _additional_stats_dict.update({"max_drawdown_dodge": self.max_drawdown_dodge})
#         for ticker in self.tickers:
#             _additional_stats_dict.update({"max_stock_price_"+ticker: self.max_stock_price.get(ticker)})
#             _additional_stats_dict.update({"max_drawdown_price_"+ticker: self.max_drawdown_price.get(ticker)})
#
#         if GrossPositionValue > 0:
#             buffer = ExcessLiquidity/ GrossPositionValue
#             _additional_stats_dict.update({"ExcessLiquidity/ GrossPositionValue":buffer})
#
#         return _additional_stats_dict
#
#     def liquidate_all_pos(self, stock_data_dict, timestamp):
#         action_msgs = []
#         for ticker in self.tickers:
#             ticker_price = stock_data_dict.get(ticker)
#             ticker_item = self.acc_data.get_portfolio_ticker_item(ticker)
#             target_share_sell = ticker_item.get("position")
#             if (target_share_sell > 0):
#                 sim_metadata = {"timestamp":timestamp}
#                 action_msg = self.trade_agent.place_sell_stock_mkt_order(ticker, target_share_sell, sim_metadata)
#                 action_msgs.append(action_msg)
#
#                 self.liq_sold_price_dict.update({ticker:ticker_price})
#                 self.liq_sold_price_qty_dict.update({ticker:target_share_sell})
#                 print("liq_sold_price_dict:",self.liq_sold_price_dict)
#                 print("liq_sold_price_qty_dict:", self.liq_sold_price_qty_dict)
#             else:
#                 continue
#         return action_msgs
#
#     def price_drawdown_calculation(self, stock_data_dict):
#
#         for ticker in self.tickers:
#             stock_price = stock_data_dict.get(ticker)
#             if self.max_stock_price.get(ticker) == None:
#                 self.max_stock_price.update({ticker: stock_price})
#                 drawdown_price = stock_price * (1 - self.max_drawdown_ratio)
#                 self.max_drawdown_price.update({ticker: drawdown_price})
#             else:
#                 # renew_max_stock_price = self.max_stock_price.get(ticker) * 0.99
#                 # if stock_price > self.max_stock_price.get(ticker):
#                 #     self.max_stock_price.update({ticker: stock_price})
#                 #     drawdown_price = stock_price * (1-self.max_drawdown_ratio)
#                 #     self.max_drawdown_price.update({ticker: drawdown_price})
#                 # elif stock_price < renew_max_stock_price:
#                 #     self.max_stock_price.update({ticker: stock_price})
#                 #     drawdown_price = stock_price * (1 - self.max_drawdown_ratio)
#                 #     self.max_drawdown_price.update({ticker: drawdown_price})
#                 #     self.liq_sold_price_dict.update({ticker: stock_price})
#                 if stock_price > self.max_stock_price.get(ticker):
#                     self.max_stock_price.update({ticker: stock_price})
#                     drawdown_price = stock_price * (1-self.max_drawdown_ratio)
#                     self.max_drawdown_price.update({ticker: drawdown_price})
#         pass
#
#     def check_max_drawdown_dodge(self, stock_data_dict):
#         for ticker in self.tickers:
#             ticker_price = stock_data_dict.get(ticker)
#             ticker_max_drawdown_price = self.max_drawdown_price.get(ticker)
#             if ticker_price < ticker_max_drawdown_price:
#                 return True
#             else:
#                 return False
#
#     def check_if_shifting_max_drawdown_price(self, ticker, ticker_price):
#         if ticker_price < self.max_drawdown_price.get(ticker)*0.9:
#             self.max_stock_price.update({ticker: ticker_price})
#             drawdown_price = ticker_price * (1 - self.max_drawdown_ratio)
#             self.max_drawdown_price.update({ticker: drawdown_price})
#
#             return True
#         else:
#             return False
#
#
#     def get_acct_snapshot(self):
#         snapshot_dict = {}
#         if len(self.acc_data.portfolio) == 0:
#             snapshot_dict = {
#                 "TotalCashValue": self.acc_data.mkt_value.get("TotalCashValue"),
#                 "GrossPositionValue": 0,
#                 "ExcessLiquidity": self.acc_data.trading_funds.get("ExcessLiquidity"),
#                 "BuyingPower": self.acc_data.trading_funds.get("BuyingPower"),
#                 "AvailableFunds": self.acc_data.trading_funds.get("AvailableFunds"),
#                 "FullInitMarginReq": 0,
#                 "Leverage": self.acc_data.trading_funds.get("Leverage"),
#                 "NetLiquidation": self.acc_data.mkt_value.get("NetLiquidation"),
#             }
#         else:
#             snapshot_dict.update({
#                 "TotalCashValue": self.acc_data.mkt_value.get("TotalCashValue"),
#                 "GrossPositionValue": self.acc_data.mkt_value.get("GrossPositionValue"),
#                 "ExcessLiquidity": self.acc_data.trading_funds.get("ExcessLiquidity"),
#                 "BuyingPower": self.acc_data.trading_funds.get("BuyingPower"),
#                 "AvailableFunds": self.acc_data.trading_funds.get("AvailableFunds"),
#                 "FullInitMarginReq": self.acc_data.margin_acc.get("FullInitMarginReq"),
#                 "Leverage": self.acc_data.trading_funds.get("Leverage"),
#                 "NetLiquidation": self.acc_data.mkt_value.get("NetLiquidation"),
#             })
#             tickers = [r['ticker'] for r in self.acc_data.portfolio]
#             for ticker in tickers:
#                 ticker_item = self.acc_data.get_portfolio_ticker_item(ticker)
#                 snapshot_dict.update({
#                     ticker + "_position": ticker_item.get("position"),
#                     ticker + "_marketPrice": ticker_item.get("marketPrice"),
#                     ticker + "_marketValue": ticker_item.get("marketValue"),
#                     ticker + "_averageCost": ticker_item.get("averageCost")
#                 })
#         return snapshot_dict
#
#     def check_buy_back(self, ticker, ticker_price):
#         if ticker_price >= self.liq_sold_price_dict.get(ticker):
#             return True
#         else:
#             return False
#
#     def buy_back_pos(self, ticker, ticker_price, orig_buy_pwr, sim_metadata):
#         if ticker_price >= self.liq_sold_price_dict.get(ticker):
#             print("ticker_price >= liq_sold_price")
#             liq_sold_price_qty = self.liq_sold_price_qty_dict.get(ticker)
#             purchase_amount = liq_sold_price_qty * ticker_price
#             if orig_buy_pwr >= purchase_amount:
#                 action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker, liq_sold_price_qty, sim_metadata)
#             else:
#                 target_share_purchase = math.floor(orig_buy_pwr / ticker_price)
#                 action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker, target_share_purchase, sim_metadata)
#
#
#
#         return action_msg
#
#
