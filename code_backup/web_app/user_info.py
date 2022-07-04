import pathlib


class info(object):
    user_id = 0
    path = ""
    table_name = ""
    run_file_dir = ""
    stats_data_dir = ""
    acc_data_dir = ""
    transact_data_dir = ""

    def __init__(self,user_id):
        path = str(pathlib.Path(__file__).parent.parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"
        table_name = f"backtest_rebalance_margin_wif_max_drawdown_control_{user_id}"
        self.user_id = user_id
        self.path = path
        self.table_name = table_name
        self.run_file_dir = f"{path}/{table_name}/run_data/"
        self.stats_data_dir = f"{path}/{table_name}/stats_data/"
        self.acc_data_dir = f"{path}/{table_name}/acc_data/"
        self.transact_data_dir = f"{path}/{table_name}/transaction_data/"
    pass