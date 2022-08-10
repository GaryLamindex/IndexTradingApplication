import pathlib

from engine.stat_engine.statistic_engine import realtime_statistic_engine


def cal_stat_function():
    trader_name = trader_name
    subscribers_num = subscribers_num
    margin_ratio = margin_ratio
    rating_dict = rating_dict
    tags_array = tags_array
    documents_link = documents_link
    video_link = video_link
    strategy_initial = strategy_initial
    store_mongoDB = store_mongoDB
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

