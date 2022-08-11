import pathlib
from datetime import datetime

import pandas as pd

from engine.stat_engine.statistic_engine import realtime_statistic_engine


class realtime_stat_application:
    def __init__(self, start_date, user_id, spec_str):
        store_mongoDB = True,
        strategy_initial = 'this is 20 80 m and msft portfolio',
        video_link = 'https://www.youtube.com',
        documents_link = 'https://google.com',
        tags_array = None,
        subscribers_num = 3,
        rating_dict = None,
        margin_ratio = 3.24,
        trader_name = 'Fai'
        table_info = {"mode": "backtest", "strategy_name": "portfolio_rebalance", "user_id": user_id}
        table_name = table_info.get("mode") + "_" + table_info.get("strategy_name") + "_" + str(
            table_info.get("user_id"))
        path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"
        start_timestamp = datetime.timestamp(start_date)
        run_file_dir = f"{path}/{table_name}/run_data/"
        stats_data_dir = f"{path}/{table_name}/stats_data/"
        self.run_file = run_file_dir + spec_str + '.csv'
        df = pd.read_csv(self.run_file)
        last_day = df["date"].iloc[-1]
        end_timestamp = datetime.timestamp(last_day)
        self.realtime_stat_engine = realtime_statistic_engine(run_file_dir, start_timestamp, end_timestamp, path,
                                                              table_name,
                                                              store_mongoDB, stats_data_dir,
                                                              strategy_initial, video_link, documents_link, tags_array,
                                                              rating_dict, margin_ratio, subscribers_num,
                                                              trader_name)

    def cal_stat_function(self):
        df = pd.read_csv(self.run_file)
        last_day = df["date"].iloc[-1]
        end_timestamp = datetime.timestamp(last_day)
        self.realtime_stat_engine.end_timestamp = end_timestamp
        self.realtime_stat_engine.cal_file_return(self.run_file)
