import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.resolve()))
from engine.visualisation_engine import graph_plotting_engine
import datetime as dt
from algo.portfolio_rebalance.backtest import \
    backtest as portfolio_rebalance_backtest
from algo.rebalance_margin_wif_max_drawdown_control.backtest import \
    backtest as rebalance_margin_wif_max_drawdown_control_backtest
from algo.rebalance_margin_never_sell.backtest import backtest as rebalance_margin_never_sell_backtest
from algo.rebalance_margin_wif_maintainance_margin.backtest import \
    backtest as rebalance_margin_wif_maintainance_margin_backtest

from engine.simulation_engine import sim_data_io_engine
from engine.simulation_engine.statistic_engine import statistic_engine


start_date = dt.datetime(2012, 10, 26)  # YYMMDD
end_date = dt.datetime(2022, 4, 29)  # YYMMDD

strategy = "portfolio_rebalance"
mode = "backtest"
cal_stat = True
quick_test = True
wipe_previous_sim_data = True
db_mode = {"dynamo_db": False, "local": True}
data_freq = "one_min"
user_id = 0
portfolio_rebalance = portfolio_rebalance_backtest()




#tickers = ['3188']
 #dataFreq = ["1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins", "1 hour", "2 hours", "3 hours", "4 hours", "8 hours", "1 day", "1W", "1M"]
# initial_amount = 1000000
# start_date = dt.datetime(2012, 10, 26)  # YYMMDD
# end_date = dt.datetime(2022, 4, 29)  # YYMMDD
#
# strategy = "rebalance_margin_wif_max_drawdown_control"
# mode = "backtest"
# cal_stat = True
# quick_test = True
# wipe_previous_sim_data = True
# rabalance_dict = {"start": 60, "end": 70, "step": 10}
# maintain_dict = {"start": 50, "end": 60, "step": 10}
# max_drawdown_ratio_dict = {"start": 5, "end": 7, "step": 2}
# purchase_exliq_ratio_dict = {"start": 500, "end": 502, "step": 2}
# db_mode = {"dynamo_db": False, "local": True}
# data_freq = "one_min"
# user_id = 0
# rebalance_margin_wif_max_drawdown_control_backtest = rebalance_margin_wif_max_drawdown_control_backtest(tickers,
#                                                                                                         initial_amount,
#                                                                                                         start_date,
#                                                                                                         end_date,
#                                                                                                         cal_stat,
#                                                                                                         rabalance_dict,
#                                                                                                         maintain_dict,
#                                                                                                         max_drawdown_ratio_dict,
#                                                                                                         purchase_exliq_ratio_dict,
#                                                                                                         data_freq,
#                                                                                                         user_id,
#                                                                                                         db_mode,
#                                                                                                         quick_test)
# rebalance_margin_wif_max_drawdown_control_backtest.loop_through_param()

# tickers = ['SPY']
# # dataFreq = ["1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins", "1 hour", "2 hours", "3 hours", "4 hours", "8 hours", "1 day", "1W", "1M"]
# initial_amount = 1000000
# start_date = dt.datetime(2007, 1, 1)  # YYMMDD
# end_date = dt.datetime(2009, 1, 1)  # YYMMDD
#
# strategy = "rebalance_margin_never_sell"
# mode = "backtest"
# cal_stat = False
# wipe_previous_sim_data = True
# rabalance_dict = {"start":200, "end":202, "step":2}
# maintain_dict = {"start":1, "end":2, "step":1}
# purchase_exliq_ratio_dict = {"start":500, "end":502, "step":2}
# db_mode = {"dynamo_db":False, "local":True}
# data_freq = "one_min"
# user_id = 0
# backtest = rebalance_margin_never_sell_backtest(tickers, initial_amount, start_date, end_date, cal_stat, rabalance_dict, maintain_dict, purchase_exliq_ratio_dict, data_freq, user_id, db_mode)
# backtest.loop_through_param()

# tickers = ['QQQ']
# # dataFreq = ["1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins", "1 hour", "2 hours", "3 hours", "4 hours", "8 hours", "1 day", "1W", "1M"]
# initial_amount = 1000000
# start_date = dt.datetime(2007, 1, 1)  # YYMMDD
# end_date = dt.datetime(2009, 1, 1)  # YYMMDD
#
# strategy = "rebalance_margin_never_sell"
# mode = "backtest"
# cal_stat = False
# wipe_previous_sim_data = True
# rabalance_dict = {"start":200, "end":202, "step":2}
# maintain_dict = {"start":200, "end":202, "step":2}
# purchase_exliq_ratio_dict = {"start":500, "end":502, "step":2}
# db_mode = {"dynamo_db":False, "local":True}
# data_freq = "one_min"
# user_id = 0
# backtest = rebalance_margin_wif_maintainance_margin_backtest(tickers, initial_amount, start_date, end_date, cal_stat, rabalance_dict, maintain_dict, purchase_exliq_ratio_dict, data_freq, user_id, db_mode)
# backtest.loop_through_param()
