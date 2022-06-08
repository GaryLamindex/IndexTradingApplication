import os
import pathlib
import datetime as dt
import requests
from zipfile import ZipFile, BadZipFile
import pandas as pd


class crypto_data_engine:
    def __init__(self):
        self.ticker_data_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.resolve()) + '/ticker_data/one_min'
        self.binance_base_url = 'https://data.binance.vision'
        self.binance_data_col_names = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                                       'Quote asset volume',
                                       'Number of trades', 'Taker buy base asset volume',
                                       'Taker buy quote asset volume', 'Ignore']

    # timestamp specifies a certain time
    # however, we only need the date
    # we will return the date string from the timestamp in the given timezone, tz and in the given format, fmt
    def get_date_str_from_timestamp(self, timestamp, tz, fmt):
        year = dt.datetime.fromtimestamp(timestamp, tz=tz).year
        month = dt.datetime.fromtimestamp(timestamp, tz=tz).month
        day = dt.datetime.fromtimestamp(timestamp, tz=tz).day
        return dt.datetime(year, month, day).strftime(fmt)

    def download_binance_daily_data(self, ticker, timestamp, bar_size):
        ticker = ticker.upper()
        date_str = self.get_date_str_from_timestamp(timestamp, dt.timezone.utc, '%Y-%m-%d')
        url = f'{self.binance_base_url}/data/spot/daily/klines/{ticker}/{bar_size}/{ticker}-{bar_size}-{date_str}.zip'
        request = requests.get(url)

        zip_filename = f'{self.ticker_data_path}/{ticker}.zip'

        with open(zip_filename, 'wb') as myzip:
            myzip.write(request.content)

        try:
            with ZipFile(zip_filename, 'r') as myzip:
                myzip.extractall(f'{self.ticker_data_path}')
                csv_filename = myzip.namelist()[0]
        except BadZipFile:
            return None
        finally:
            os.remove(zip_filename)

        return f'{self.ticker_data_path}/{csv_filename}'

    def get_historical_data_by_range(self, tickers, start_timestamp, end_timestamp, bar_size):
        for ticker in tickers:
            ticker = ticker.upper()

            merging_dfs = [None, None]
            current_timestamp = start_timestamp
            while current_timestamp <= end_timestamp:
                csv_filename = self.download_binance_daily_data(ticker, current_timestamp, bar_size)
                if merging_dfs[0] is None:
                    merging_dfs[0] = pd.read_csv(csv_filename, names=self.binance_data_col_names)

                elif merging_dfs[1] is None:
                    merging_dfs[1] = pd.read_csv(csv_filename, names=self.binance_data_col_names)

                os.remove(csv_filename)

                if merging_dfs[0] is not None and merging_dfs[1] is not None:
                    merging_dfs[0] = pd.concat(merging_dfs, ignore_index=True)
                    merging_dfs[1] = None
                current_timestamp += 86400   # differ by one day

            result_df = merging_dfs[0]
            result_df['Open time'] = round(result_df['Open time'] / 1000)
            result_df['Close time'] = round(result_df['Close time'] / 1000)
            result_df.to_csv(f'{self.ticker_data_path}/{ticker}.csv', index=False)


def main():
    engine = crypto_data_engine()
    tickers = ['ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT', 'SOLUSDT', 'DOGEUSDT', 'DOTUSDT', 'TRXUSDT',
               'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT']
    engine.get_historical_data_by_range(tickers, 1614556800, 1654473600, '1m')


if __name__ == '__main__':
    main()
