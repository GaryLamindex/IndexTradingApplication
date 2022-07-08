import os
import pathlib
import pandas as pd


class crypto_local_engine:
    def __init__(self, ticker):
        self.crypto_daily_path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + \
                                 '/ticker_data/crypto_daily'
        self.ticker = ticker.upper()
        self.df_path = f'{self.crypto_daily_path}/{self.ticker}.csv'
        self.full_ticker_df = pd.read_csv(self.df_path, engine='pyarrow')
        self.full_ticker_df['Date'] = pd.to_datetime(self.full_ticker_df['Date'], format='%Y-%m-%d')
        if 'timestamp' not in self.full_ticker_df.columns:
            self.full_ticker_df['timestamp'] = self.full_ticker_df['Date'].apply(lambda x: int(x.timestamp()))
            os.remove(self.df_path)
            self.full_ticker_df.to_csv(self.df_path)

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
        while row_df.empty and self.full_ticker_df['timestamp'].iloc[0] < timestamp:
            row_df = self.full_ticker_df.loc[self.full_ticker_df['timestamp'] == timestamp - 86400, field]
        return None if row_df.empty else row_df.item()

    def get_data_by_timestamp(self, timestamp):
        df = self.full_ticker_df
        return df[df['timestamp'] == timestamp]
