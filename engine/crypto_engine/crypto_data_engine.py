import os
import pathlib
import datetime as dt
import requests
from zipfile import ZipFile, BadZipFile
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


class crypto_data_engine:
    def __init__(self):
        self.ticker_data_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.resolve()) + '/ticker_data/one_min'
        self.crypto_market_cap_data_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.resolve()) + '/ticker_data/crypto_market_cap'
        self.binance_base_url = 'https://data.binance.vision'
        self.coingecko_ranking_url = 'https://www.coingecko.com/'
        self.coingecko_historical_data_url = 'https://www.coingecko.com/en/coins/cardano/historical_data?end_date=2022-06-08&start_date=2021-03-01#panel'
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

    # e.g. convert '$582,266,289,816' to int type
    # doesn't support number with decimal point
    # only integer
    def get_extract_from_format(self, string):
        result_str = ''
        for c in string:
            if c.isdigit():
                result_str += c
        return int(result_str)

    def get_coingecko_historical_market_cap_url(self, coin, start_timestamp, end_timestamp, page):
        start_date_string = self.get_date_str_from_timestamp(start_timestamp, dt.timezone.utc, '%Y-%m-%d')
        end_date_string = self.get_date_str_from_timestamp(end_timestamp, dt.timezone.utc, '%Y-%m-%d')
        url = f'https://www.coingecko.com/en/coins/{coin}/historical_data?start_date={start_date_string}' \
              f'&end_date={end_date_string}&page={page}'
        return url

    def crawl_historical_market_cap_by_range_coingecko(self, start_timestamp, end_timestamp):
        browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        browser.get(self.coingecko_ranking_url)
        elements = browser.find_elements(by=By.TAG_NAME, value='td')

        rank_df = pd.DataFrame(columns=['FullName', 'Symbol'])

        i = 0
        for e in elements:
            if e.text == '' and (i <= 1 or i > 8):
                i = 0
                continue
            if i == 1:
                row = e.text.split('\n')
                temp_df = pd.DataFrame({'FullName': [row[0].replace(' ', '-').lower()], 'Symbol': [row[1]]})
                rank_df = pd.concat([rank_df, temp_df], ignore_index=True)
            i = i + 1

        i = 1
        for coin in rank_df['FullName']:
            browser.get(self.get_coingecko_historical_market_cap_url(coin, start_timestamp, end_timestamp, i))
            elements = browser.find_elements(by=By.TAG_NAME, value='tr')
            elements = elements[1:]   # drop the header
            market_cap_df = pd.DataFrame(columns=['Date', 'MarketCap'])
            for e in elements:
                row = e.text.split(' ')
                temp_df = pd.DataFrame({'Date': row[0], 'MarketCap': row[1]})
                market_cap_df = pd.concat([market_cap_df, temp_df], ignore_index=True)



        browser.quit()


def main():
    engine = crypto_data_engine()
    tickers = ['ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT', 'SOLUSDT', 'DOGEUSDT', 'DOTUSDT', 'TRXUSDT',
               'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT']
    # engine.get_historical_data_by_range(tickers, 1614556800, 1654473600, '1m')
    engine.crawl_historical_market_cap_by_range_coingecko(1614556800, 1654473600)


if __name__ == '__main__':
    main()
