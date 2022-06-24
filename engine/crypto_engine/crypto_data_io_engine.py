import pathlib
import pandas as pd


class crypto_local_engine:
    def __init__(self, ticker):
        self.crypto_daily_path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + \
                                 '/ticker_data/crypto_daily'
        self.ticker = ticker.upper()
        self.full_ticker_df = pd.read_csv(f'{self.crypto_daily_path}/{self.ticker}.csv')
        self.full_ticker_df['Date'] = pd.to_datetime(self.full_ticker_df['Date'], format='%Y-%m-%d')
        self.full_ticker_df['timestamp'] = self.full_ticker_df['Date'].apply(lambda x: int(x.timestamp()))

    def get_n_days_data(self, timestamp, n):
        start_timestamp = timestamp - 86400 * n
        return self.full_ticker_df[start_timestamp <= self.full_ticker_df['timestamp'] <= timestamp]

    def get_full_ticker_df(self):
        return self.full_ticker_df

    def get_data_by_range(self, range):
        return self.full_ticker_df[range[0] <= self.full_ticker_df['timestamp'] <= range[1]]

    def get_ticker_item_by_timestamp(self, timestamp):
        ticker_row = self.full_ticker_df[self.full_ticker_df['timestamp'] == timestamp].reset_index(drop=True)
        if ticker_row.empty:
            column_names = ticker_row.columns
            return {name: None for name in column_names}
        else:
            return ticker_row.to_dict(orient='index')[0]

    def get_field_by_timestamp(self, timestamp, field):
        row_df = self.full_ticker_df.loc[self.full_ticker_df['timestamp'] == timestamp, field]
        if row_df.empty:
            return None
        return row_df.item()

    def get_pct_change_by_timestamp(self, periods, timestamp):
        df = self.get_full_ticker_df()
        df['pct_change'] = df['Open'].pct_change(periods=periods)
        row_df = df.loc[df['timestamp'] == timestamp, 'pct_change']
        if row_df.empty:
            return None
        return row_df.item()

    def get_data_by_timestamp(self, timestamp):
        df = self.full_ticker_df
        return df[df['timestamp'] == timestamp]
