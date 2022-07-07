import json
from ast import literal_eval
import sys
import pathlib
from crypto_algo.momentum_strategy_crypto.backtest import backtest as momentum_strategy_backtest
import datetime as dt
import pandas as pd
import os

sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.resolve()))

# from algo.rebalance_margin_wif_max_drawdown_control.backtest import \
#     backtest as rebalance_margin_wif_max_drawdown_control_backtest
# from algo.rebalance_margin_never_sell.backtest import backtest as rebalance_margin_never_sell_backtest
# from algo.rebalance_margin_wif_maintainance_margin.backtest import \
#     backtest as rebalance_margin_wif_maintainance_margin_backtest
#
# from engine.simulation_engine import sim_data_io_engine
# from engine.simulation_engine.statistic_engine import statistic_engine

path = str(pathlib.Path(__file__).parent.parent.parent.resolve()) + '/ticker_data/crypto_daily'
tickers = ['BTC', 'SHIB', 'ETH', 'BURGER', 'VGX']
# filelist = os.listdir(path)
# for filename in filelist:
#     if not filename.startswith('.'):
#         tickers.append(os.path.splitext(filename)[0])
initial_amount = 100000
start_date = dt.datetime(2017, 1, 1, tzinfo=dt.timezone.utc)
end_date = dt.datetime(2022, 6, 23, tzinfo=dt.timezone.utc)
periods_dict = {"start": 20, "end": 21, "step": 1}
cal_stat = True
user_id = 0
db_mode = {"dynamo_db": False, "local": True}
backtest = momentum_strategy_backtest(tickers, initial_amount, start_date, end_date,
                                      cal_stat, user_id, periods_dict, db_mode)
backtest.loop_through_params()


# start_date = dt.datetime(2010, 1, 1)  # YYMMDD
# end_date = dt.datetime(2011, 3, 15)  # YYMMDD

# strategy = "portfolio_rebalance"
# mode = "backtest"
# initial_amount = 10000
# start_date = dt.datetime(2015, 1, 1, tzinfo=dt.timezone.utc)
# end_date = dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc)
# periods_dict = {"start": 20, "end": 21, "step": 1}
# cal_stat = True
# user_id = 0
# db_mode = {"dynamo_db": False, "local": True}
# backtest = momentum_strategy_backtest(tickers, initial_amount, start_date, end_date,
#                                       cal_stat, user_id, periods_dict, db_mode)
# backtest.loop_through_params()

# from algo.portfolio_rebalance.backtest import backtest as portfolio_rebalance_backtest
#
# start_date = dt.datetime(2010, 1, 1)  # YYMMDD
# end_date = dt.datetime(2011, 5, 15)  # YYMMDD
#
# strategy = "portfolio_rebalance"
# mode = "backtest"
# cal_stat = True
# quick_test = True
# wipe_previous_sim_data = True
# db_mode = {"dynamo_db": False, "local": True}
# data_freq = "one_min"
# user_id = 0


from algo.portfolio_rebalance.backtest import backtest as portfolio_rebalance_backtest

start_date = dt.datetime(2018, 1, 1)  # YYMMDD
end_date = dt.datetime(2019, 3, 15)  # YYMMDD

strategy = "portfolio_rebalance"
mode = "backtest"
cal_stat = True
quick_test = True
wipe_previous_sim_data = True
db_mode = {"dynamo_db": False, "local": True}
data_freq = "one_min"
user_id = 0
df = pd.read_csv('/Users/percychui/Downloads/scraper2.csv')
df = df.loc[df["File Missed"] == "[]"]
dict = df["Json"].tolist()
tmp = list()
tickers = list()
weight = list()
for x in range(len(dict)):
    dict[x] = literal_eval(dict[x])
check = list()
for x in range(len(dict)):
    check.append(len(dict[x]))
df["Check"] = check
df = df.loc[df["Check"] != 1]
dict = df["Json"].tolist()
for x in range(len(dict)):
    dict[x] = literal_eval(dict[x])

for x in range(len(dict)):
    for y in range(len(dict[x])):
        dict[x][y] = json.loads(dict[x][y])
        tmp.append(list(dict[x][y].keys()))
    tmp = [x for xs in tmp for x in xs]
    tickers.append(tmp.copy())
    tmp.clear()
for x in range(len(dict)):
    for y in range(len(dict[x])):
        tmp1 = list(dict[x][y].values())
        tmp1 = [float(x) for x in tmp1]
        tmp.append(tmp1)
    tmp = [x for xs in tmp for x in xs]
    weight.append(tmp.copy())
    tmp.clear()

weight = list(map(lambda x: [x], weight))
# tickers = ["M", "MSFT"]
deposit_amount = 1000000
acceptance_range = 0
num_tickers = len(tickers)
# rebalance_ratio = portfolio_rebalance_backtest.get_outcomes(num_tickers, 100)
rebalance_ratio = weight
# rebalance_ratio = [[50, 50]]
print(weight)
print(tickers)
for x in range(len(dict)):
    portfolio_rebalance = portfolio_rebalance_backtest(tickers[x],
                                                       deposit_amount,
                                                       start_date,
                                                       end_date,
                                                       cal_stat,
                                                       data_freq,
                                                       user_id,
                                                       db_mode,
                                                       quick_test,
                                                       acceptance_range, rebalance_ratio[x], store_mongoDB=True,
                                                       strategy_initial='this is 20 80 m and msft portfolio',
                                                       video_link='https://www.youtube.com',
                                                       documents_link='https://google.com',
                                                       tags_array=None,
                                                       subscribers_num=3,
                                                       rating_dict=None,
                                                       margin_ratio=3.24,
                                                       trader_name='Fai')
    portfolio_rebalance.loop_through_param()
# tickers = ["M", "MSFT"]
# deposit_amount = 1000000
# acceptance_range = 0
# num_tickers = len(tickers)
# #rebalance_ratio = portfolio_rebalance_backtest.get_outcomes(num_tickers, 100)
# rebalance_ratio = [[20, 80]]
#
# portfolio_rebalance = portfolio_rebalance_backtest(tickers,
#                                                    deposit_amount,
#                                                    start_date,
#                                                    end_date,
#                                                    cal_stat,
#                                                    data_freq,
#                                                    user_id,
#                                                    db_mode,
#                                                    quick_test,
#                                                    acceptance_range, rebalance_ratio,
#                                                    store_mongoDB=True,
#                                                    strategy_initial='this is 20 80 m and msft portfolio',
#                                                    video_link='https://www.youtube.com',
#                                                    documents_link='https://google.com',
#                                                    tags_array=None,
#                                                    subscribers_num=3,
#                                                    rating_dict=None,
#                                                    margin_ratio=3.24,
#                                                    trader_name='Fai'
#                                                    )
#
# portfolio_rebalance.loop_through_param()


# tickers = ['3188']
# dataFreq = ["1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins", "1 hour", "2 hours", "3 hours", "4 hours", "8 hours", "1 day", "1W", "1M"]
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
#                                                                                                         quick_test,
#                                                                                                         store_mongoDB=True)
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
