class crypto_acc_data():
    user_id = 0

    def __init__(self, user_id, strategy_name, table_name, spec_str):
        self.user_id = user_id
        self.strategy_name = strategy_name
        self.table_name = table_name
        self.portfolio = []
        self.mkt_value = {'TotalCashValue': 0, 'NetLiquidation': 0}

    

