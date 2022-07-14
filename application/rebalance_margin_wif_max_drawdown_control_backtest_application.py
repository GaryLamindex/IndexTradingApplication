import datetime as dt
from algo.rebalance_margin_wif_max_drawdown_control.backtest import backtest as rebalance_margin_wif_max_drawdown_control_backtest

tickers = ['QQQ']
dataFreq = ["1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins", "1 hour", "2 hours", "3 hours", "4 hours", "8 hours", "1 day", "1W", "1M"]
initial_amount = 1000000
start_date = dt.datetime(2012, 10, 26)  # YYMMDD
end_date = dt.datetime(2022, 4, 29)  # YYMMDD

strategy = "rebalance_margin_wif_max_drawdown_control"
mode = "backtest"
cal_stat = True
quick_test = True
wipe_previous_sim_data = True
rabalance_dict = {"start": 60, "end": 70, "step": 10}
maintain_dict = {"start": 50, "end": 60, "step": 10}
max_drawdown_ratio_dict = {"start": 5, "end": 7, "step": 2}
purchase_exliq_ratio_dict = {"start": 500, "end": 502, "step": 2}
db_mode = {"dynamo_db": False, "local": True}
data_freq = "one_min"
user_id = 0
rebalance_margin_wif_max_drawdown_control_backtest = rebalance_margin_wif_max_drawdown_control_backtest(tickers,
                                                                                                        initial_amount,
                                                                                                        start_date,
                                                                                                        end_date,
                                                                                                        cal_stat,
                                                                                                        rabalance_dict,
                                                                                                        maintain_dict,
                                                                                                        max_drawdown_ratio_dict,
                                                                                                        purchase_exliq_ratio_dict,
                                                                                                        data_freq,
                                                                                                        user_id,
                                                                                                        db_mode,
                                                                                                        quick_test,
                                                                                                        store_mongoDB=False)
rebalance_margin_wif_max_drawdown_control_backtest.loop_through_param()
