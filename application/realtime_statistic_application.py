import pathlib

from engine.stat_engine.statistic_engine import realtime_statistic_engine


def cal_stat_function():
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
    table_name = table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
        table_info.get("user_id"))
    path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"
    end_timestamp = end_timestamp
    start_timestamp = start_timestamp
    run_file_dir = f"{path}/{table_name}/run_data/"
    stats_data_dir = f"{path}/{table_name}/stats_data/"
    realtime_stat_engine = realtime_statistic_engine(run_file_dir, start_timestamp, end_timestamp, path, table_name, store_mongoDB, stats_data_dir,
                 strategy_initial, video_link, documents_link, tags_array, rating_dict, margin_ratio, subscribers_num,
                 trader_name)
    realtime_stat_engine.cal_all_file_return()

