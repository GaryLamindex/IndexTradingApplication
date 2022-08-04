import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from engine.mongoDB_engine.write_document_engine import Write_Mongodb
from engine.simulation_engine import sim_data_io_engine
from engine.stat_engine.statistic_engine import statistic_engine


class realtime_statistic_engine:

    def __init__(self, run_file_dir, start_timestamp, end_timestamp, path, table_name, store_mongoDB, stats_data_dir,
                 strategy_initial, video_link, documents_link, tags_array, rating_dict, margin_ratio, subscribers_num,
                 trader_name):

        self.trader_name = trader_name
        self.subscribers_num = subscribers_num
        self.margin_ratio = margin_ratio
        self.rating_dict = rating_dict
        self.tags_array = tags_array
        self.documents_link = documents_link
        self.video_link = video_link
        self.strategy_initial = strategy_initial
        self.stats_data_dir = stats_data_dir
        self.store_mongoDB = store_mongoDB
        self.table_name = table_name
        self.path = path
        self.end_timestamp = end_timestamp
        self.start_timestamp = start_timestamp
        self.run_file_dir = run_file_dir

    def cal_all_file_return(self):
        sim_data_offline_engine = sim_data_io_engine.offline_engine(self.run_file_dir)
        backtest_data_directory = os.fsencode(self.run_file_dir)
        data_list = []
        for idx, file in enumerate(os.listdir(backtest_data_directory)):
            if file.decode().endswith("csv"):
                ticker_name = file.decode().split("_")
                marketCol = f'marketPrice_{ticker_name[1]}'
                # costCol = f'costBasis_{self.tickers[idx]}'
                # valueCol = f'marketValue_{self.tickers[idx]}'
                file_name = file.decode().split(".csv")[0]
                stat_engine = statistic_engine(sim_data_offline_engine)
                # stat_engine_3 = statistic_engine_3(sim_data_offline_engine)
                sharpe_dict = stat_engine.get_sharpe_data(file_name)
                inception_sharpe = sharpe_dict.get("inception")
                _1_yr_sharpe = sharpe_dict.get("1y")
                _3_yr_sharpe = sharpe_dict.get("3y")
                _5_yr_sharpe = sharpe_dict.get("5y")
                _ytd_sharpe = sharpe_dict.get("ytd")

                sortino_dict = stat_engine.get_sortino_data(file_name)
                inception_sortino = sortino_dict.get('inception')
                _1_yr_sortino = sortino_dict.get('1y')
                _3_yr_sortino = sortino_dict.get('3y')
                _5_yr_sortino = sortino_dict.get('5y')
                _ytd_sortino = sortino_dict.get('ytd')

                return_dict, return_inflation_adj_dict, compound_return_dict = stat_engine.get_return_data(file_name)
                inception_return = return_dict.get("inception")
                _1_yr_return = return_dict.get("1y")
                _3_yr_return = return_dict.get("3y")
                _5_yr_return = return_dict.get("5y")
                _ytd_return = return_dict.get("ytd")
                inflation_adj_inception_return = return_inflation_adj_dict.get('inception')
                inflation_adj_1_yr_return = return_inflation_adj_dict.get('1y')
                inflation_adj_3_yr_return = return_inflation_adj_dict.get('3y')
                inflation_adj_5_yr_return = return_inflation_adj_dict.get('5y')
                inflation_adj_ytd_return = return_inflation_adj_dict.get('ytd')
                compound_inception_return_dict = compound_return_dict.get('inception')
                compound_1_yr_return_dict = compound_return_dict.get('1y')
                compound_3_yr_return_dict = compound_return_dict.get('3y')
                compound_5_yr_return_dict = compound_return_dict.get('5y')
                compound_ytd_return_dict = compound_return_dict.get('ytd')

                max_drawdown_dict = stat_engine.get_max_drawdown_data(file_name)
                inception_max_drawdown = max_drawdown_dict.get("inception")
                _1_yr_max_drawdown = max_drawdown_dict.get("1y")
                _3_yr_max_drawdown = max_drawdown_dict.get("3y")
                _5_yr_max_drawdown = max_drawdown_dict.get("5y")
                _ytd_max_drawdown = max_drawdown_dict.get("ytd")

                alpha_dict = stat_engine.get_alpha_data(file_name, marketCol)
                inception_alpha = alpha_dict.get('inception')
                _1_yr_alpha = alpha_dict.get('1y')
                _3_yr_alpha = alpha_dict.get('3y')
                _5_yr_alpha = alpha_dict.get('5y')
                _ytd_alpha = alpha_dict.get('ytd')

                volatility_dict = stat_engine.get_volatility_data(file_name, marketCol)
                inception_volatility = volatility_dict.get('inception')
                _1_yr_volatility = volatility_dict.get('1y')
                _3_yr_volatility = volatility_dict.get('3y')
                _5_yr_volatility = volatility_dict.get('5y')
                _ytd_volatility = volatility_dict.get('ytd')

                win_rate_dict = stat_engine.get_win_rate_data(file_name)
                inception_win_rate = win_rate_dict.get('inception')
                _1_yr_win_rate = win_rate_dict.get('1y')
                _3_yr_win_rate = win_rate_dict.get('3y')
                _5_yr_win_rate = win_rate_dict.get('5y')
                _ytd_win_rate = win_rate_dict.get('ytd')

                dateStringS = datetime.fromtimestamp(self.start_timestamp)
                dateStringE = datetime.fromtimestamp(self.end_timestamp)
                date_range = [f"{dateStringS.year}-{dateStringS.month}-{dateStringS.day}", \
                              f"{dateStringE.year}-{dateStringE.month}-{dateStringE.day}"]
                rolling_return_dict = stat_engine.get_rolling_return_data(file_name, date_range)
                _1_yr_rolling_return = rolling_return_dict.get('1y')
                _2_yr_rolling_return = rolling_return_dict.get('2y')
                _3_yr_rolling_return = rolling_return_dict.get('3y')
                _5_yr_rolling_return = rolling_return_dict.get('5y')
                _7_yr_rolling_return = rolling_return_dict.get('7y')
                _10_yr_rolling_return = rolling_return_dict.get('10y')
                _15_yr_rolling_return = rolling_return_dict.get('15y')
                _20_yr_rolling_return = rolling_return_dict.get('20y')

                ########## Store drawdown in another csv
                drawdown_abstract, drawdown_raw_data = stat_engine.get_drawdown_data(file_name, date_range)
                drawdown_raw_data.to_csv(f"{self.path}/{self.table_name}/stats_data/{file_name}drawdown_raw_data.csv",
                                         index=False)
                drawdown_abstract.to_csv(f"{self.path}/{self.table_name}/stats_data/{file_name}drawdown_abstract.csv",
                                         index=False)
                # drawdown_dict = stat_engine.get_drawdown_data(file_name, date_range)
                # drawdown_abstract = drawdown_dict.get('drawdown_abstract')
                # drawdown_raw_data = drawdown_dict.get('drawdown_raw_data')

                average_win_day_dict = stat_engine.get_average_win_day_data(file_name)
                inception_average_win_day = average_win_day_dict.get('inception')
                _1_yr_average_win_day = average_win_day_dict.get('1y')
                _3_yr_average_win_day = average_win_day_dict.get('3y')
                _5_yr_average_win_day = average_win_day_dict.get('5y')
                _ytd_average_win_day = average_win_day_dict.get('ytd')

                profit_loss_ratio_dict = stat_engine.get_profit_loss_ratio_data(file_name)
                inception_profit_loss_ratio = profit_loss_ratio_dict.get('inception')
                _1_yr_profit_loss_ratio = profit_loss_ratio_dict.get('1y')
                _3_yr_profit_loss_ratio = profit_loss_ratio_dict.get('3y')
                _5_yr_profit_loss_ratio = profit_loss_ratio_dict.get('5y')
                _ytd_profit_loss_ratio = profit_loss_ratio_dict.get('ytd')

                last_nlv = stat_engine.get_last_nlv(file_name)
                last_daily = stat_engine.get_last_daily_change(file_name)
                last_monthly = stat_engine.get_last_daily_change(file_name)

                composite_dict, number_of_ETFs = stat_engine.get_composite_data(file_name)

                sd_dict = stat_engine.get_sd_data(file_name)
                _1_yr_sd = sd_dict.get('1y')
                _3_yr_sd = sd_dict.get('3y')
                _5_yr_sd = sd_dict.get('5y')
                inception_sd = sd_dict.get('inception')

                pos_neg_dict = stat_engine.get_pos_neg_data(file_name)
                _1_yr_pos_neg = pos_neg_dict.get('1y')
                _3_yr_pos_neg = pos_neg_dict.get('3y')
                _5_yr_pos_neg = pos_neg_dict.get('5y')
                inception_pos_neg = pos_neg_dict.get('inception')

                information_ratio_dict = stat_engine.get_information_ratio_data(file_name, marketCol)
                _1_yr_information_ratio = information_ratio_dict.get('1y')
                _3_yr_information_ratio = information_ratio_dict.get('3y')
                _5_yr_information_ratio = information_ratio_dict.get('5y')
                inception_information_ratio = information_ratio_dict.get('inception')

                net_profit = stat_engine.get_net_profit_inception(file_name)

                all_file_stats_row = {
                    "Backtest Spec": file_name, 'YTD Return': _ytd_return, '1 Yr Return': _1_yr_return,
                    "3 Yr Return": _3_yr_return, "5 Yr Return": _5_yr_return,
                    "Since Inception Return": inception_return,
                    'inflation adj YTD Return': inflation_adj_ytd_return,
                    'inflation adj 1 Yr Return': inflation_adj_1_yr_return,
                    'inflation adj 3 Yr Return': inflation_adj_3_yr_return,
                    'inflation adj 5 yr Return': inflation_adj_5_yr_return,
                    'inflation adj Inception Return': inflation_adj_inception_return,
                    "Since Inception Sharpe": inception_sharpe,
                    "YTD Sharpe": _ytd_sharpe,
                    "1 Yr Sharpe": _1_yr_sharpe, "3 Yr Sharpe": _3_yr_sharpe, "5 Yr Sharpe": _5_yr_sharpe,
                    'Since Inception Sortino': inception_sortino, 'YTD Sortino': _ytd_sortino,
                    '1 Yr Sortino': _1_yr_sortino, '3 Yr Sortino': _3_yr_sortino, '5 Yr Sortino': _5_yr_sortino,
                    "Since Inception Max Drawdown": inception_max_drawdown, "YTD Max Drawdown": _ytd_max_drawdown,
                    "1 Yr Max Drawdown": _1_yr_max_drawdown, "3 Yr Max Drawdown": _3_yr_max_drawdown,
                    "5 Yr Max Drawdown": _5_yr_max_drawdown,
                    "Since Inception Alpha": inception_alpha, "YTD Alpha": _ytd_alpha,
                    "1 Yr Alpha": _1_yr_alpha, "3 Yr Alpha": _3_yr_alpha,
                    "5 Yr Alpha": _5_yr_alpha,
                    "Since Inception Volatility": inception_volatility, "YTD Volatility": _ytd_volatility,
                    "1 Yr Volatility": _1_yr_volatility, "3 Yr Volatility": _3_yr_volatility,
                    "5 Yr Volatility": _5_yr_volatility,
                    "Since Inception Win Rate": inception_win_rate, "YTD Win Rate": _ytd_win_rate,
                    "1 Yr Win Rate": _1_yr_win_rate, "3 Yr Win Rate": _3_yr_win_rate,
                    "5 Yr Win Rate": _5_yr_win_rate,

                    "1 Yr Rolling Return": _1_yr_rolling_return, "2 Yr Rolling Return": _2_yr_rolling_return,
                    "3 Yr Rolling Return": _3_yr_rolling_return, "5 Yr Rolling Return": _5_yr_rolling_return,
                    "7 Yr Rolling Return": _7_yr_rolling_return, "10 Yr Rolling Return": _10_yr_rolling_return,
                    "15 Yr Rolling Return": _15_yr_rolling_return, "20 Yr Rolling Return": _20_yr_rolling_return,
                    # "Drawdown_abstract": drawdown_abstract, "Drawdown_raw_data": drawdown_raw_data,

                    "Since Inception Average Win Per Day": inception_average_win_day,
                    "YTD Average Win Per Day": _ytd_average_win_day, "1 Yr Average Win Per Day": _1_yr_average_win_day,
                    "3 Yr Average Win Per Day": _3_yr_average_win_day,
                    "5 Yr Average Win Per Day": _5_yr_average_win_day,
                    "Since Inception Profit Loss Ratio": inception_profit_loss_ratio,
                    "YTD Profit Loss Ratio": _ytd_profit_loss_ratio, "1 Yr Profit Loss Ratio": _1_yr_profit_loss_ratio,
                    "3 Yr Profit Loss Ratio": _3_yr_profit_loss_ratio,
                    "5 Yr Profit Loss Ratio": _5_yr_profit_loss_ratio,
                    "last nlv": last_nlv, "last daily change": last_daily, "last monthly change": last_monthly,

                    "Composite": composite_dict,
                    "number_of_ETFs": number_of_ETFs,

                    "1 yr sd": _1_yr_sd,
                    "3 yr sd": _3_yr_sd,
                    "5 yr sd": _5_yr_sd,
                    "inception sd": inception_sd,

                    "1 yr pos neg": _1_yr_pos_neg,
                    "3 yr pos neg": _3_yr_pos_neg,
                    "5 yr pos neg": _5_yr_pos_neg,
                    "inception pos neg": inception_pos_neg,
                    "1 yr information ratio": _1_yr_information_ratio,
                    "3 yr information ratio": _3_yr_information_ratio,
                    "5 yr information ratio": _5_yr_information_ratio,
                    "inception information ratio": inception_information_ratio,
                    "net profit": net_profit,
                    "compound_inception_return_dict": compound_inception_return_dict,
                    "compound_1_yr_return_dict": compound_1_yr_return_dict,
                    "compound_3_yr_return_dict": compound_3_yr_return_dict,
                    "compound_5_yr_return_dict": compound_5_yr_return_dict,
                    "compound_ytd_return_dict": compound_ytd_return_dict
                }

                # _additional_data = self.cal_additional_data(file_name)
                # data_list.append(all_file_stats_row | _additional_data)
                _additional_data = {}
                data_list.append(all_file_stats_row | _additional_data)

        col = ['Backtest Spec', 'YTD Return', '1 Yr Return', "3 Yr Return", "5 Yr Return",
               "Since Inception Return", "Since Inception Sharpe", "YTD Sharpe", "1 Yr Sharpe", "3 Yr Sharpe",
               "5 Yr Sharpe", 'Since Inception Sortino', 'YTD Sortino', '1 Yr Sortino', '3 Yr Sortino', '5 Yr Sortino',
               "Since Inception Max Drawdown", "YTD Max Drawdown",
               "1 Yr Max Drawdown",
               "3 Yr Max Drawdown", "5 Yr Max Drawdown",
               "Since Inception Alpha", "YTD Alpha", "1 Yr Alpha", "3 Yr Alpha", "5 Yr Alpha",
               "Since Inception Volatility", "YTD Volatility", "1 Yr Volatility", "3 Yr Volatility", "5 Yr Volatility",
               "Since Inception Win Rate", "YTD Win Rate", "1 Yr Win Rate", "3 Yr Win Rate", "5 Yr Win Rate",
               "1 Yr Rolling Return", "2 Yr Rolling Return", "3 Yr Rolling Return", "5 Yr Rolling Return",
               "7 Yr Rolling Return", "10 Yr Rolling Return", "15 Yr Rolling Return", "20 Yr Rolling Return",
               # "Drawdown_abstract","Drawdown_raw_data",
               "Since Inception Average Win Per Day", "YTD Average Win Per Day", "1 Yr Average Win Per Day",
               "3 Yr Average Win Per Day", "5 Yr Average Win Per Day",
               "Since Inception Profit Loss Ratio", "YTD Profit Loss Ratio", "1 Yr Profit Loss Ratio",
               "3 Yr Profit Loss Ratio", "5 Yr Profit Loss Ratio",
               "last nlv", "last daily change", "last monthly change",
               "Composite", "number_of_ETFs",
               "1 yr sd", "3 yr sd", "5 yr sd", "inception sd", "1 yr pos neg", "3 yr pos neg", "5 yr pos neg",
               "inception pos neg", "1 yr information ratio", "3 yr information ratio", "5 yr information ratio",
               "inception information ratio", "net profit",
               "compound_inception_return_dict", "compound_1_yr_return_dict", "compound_3_yr_return_dict",
               "compound_5_yr_return_dict", "compound_ytd_return_dict"
               ]

        df = pd.DataFrame(data=data_list, columns=col)
        # pd.set_option("max_colwidth", 10000)
        df.fillna(0)
        print(f"{self.path}/stats_data/{self.table_name}.csv")
        df.to_csv(f"{self.path}/{self.table_name}/stats_data/all_file_return.csv", index=False)

        # store data to mongoDB HERE
        if self.store_mongoDB:
            print("(*&^%$#$%^&*()(*&^%$#$%^&*(")
            p = Write_Mongodb()
            for file in os.listdir(backtest_data_directory):
                if file.decode().endswith("csv"):
                    csv_path = Path(self.run_file_dir, file.decode())
                    a = pd.read_csv(csv_path)
                    spec = file.decode().split('.csv')
                    name = spec[0] + "drawdown_abstract.csv"
                    name2 = spec[0] + "drawdown_raw_data.csv"
                    abstract_path = Path(self.stats_data_dir, name)
                    drawdown_abstract = pd.read_csv(abstract_path)
                    raw_data_path = Path(self.stats_data_dir, name2)
                    drawdown_raw_data = pd.read_csv(raw_data_path)
                    p.write_new_backtest_result(strategy_name=self.table_name + '_' + spec[0],
                                                drawdown_abstract_df=drawdown_abstract,
                                                drawdown_raw_df=drawdown_raw_data,
                                                run_df=a,
                                                all_file_return_df=df,
                                                strategy_initial=self.strategy_initial,
                                                video_link=self.video_link,
                                                documents_link=self.documents_link,
                                                tags_array=self.tags_array,
                                                rating_dict=self.rating_dict,
                                                margin_ratio=self.margin_ratio,
                                                subscribers_num=self.subscribers_num,
                                                trader_name=self.trader_name)
