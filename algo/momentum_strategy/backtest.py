from engine.crypto_engine.crypto_data_engine import *


class backtest:
    def __init__(self, tickers, initial_amount, start_date, end_date, cal_stat, user_id):
        self.tickers = tickers
        self.initial_amount = initial_amount
        self.start_date = start_date
        self.end_date = end_date
        self.cal_stat = cal_stat
        self.path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/backtest"
        self.crypto_data_engine = crypto_data_engine()
        pass

    def load_data_from_engine(self):
        dfs = []
        for ticker in self.tickers:
            df = self.crypto_data_engine.get_crypto_daily_data(ticker)
            if df is not None:
                dfs.append(df)
        return dfs

