import os
from os import listdir
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
        self.coingecko_ranking_url = 'https://www.coingecko.com/en/all-cryptocurrencies'
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
            csv_filename = None
            merging_dfs = [None, None]
            current_timestamp = start_timestamp
            while current_timestamp <= end_timestamp:
                csv_filename = self.download_binance_daily_data(ticker, current_timestamp, bar_size)
                if csv_filename is None:
                    break
                if merging_dfs[0] is None:
                    merging_dfs[0] = pd.read_csv(csv_filename, names=self.binance_data_col_names)

                elif merging_dfs[1] is None:
                    merging_dfs[1] = pd.read_csv(csv_filename, names=self.binance_data_col_names)

                os.remove(csv_filename)

                if merging_dfs[0] is not None and merging_dfs[1] is not None:
                    merging_dfs[0] = pd.concat(merging_dfs, ignore_index=True)
                    merging_dfs[1] = None
                current_timestamp += 86400  # differ by one day

            if csv_filename is None:
                continue

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

    def crawl_historical_market_cap_by_range_coingecko(self):
        chrome_options = webdriver.ChromeOptions()
        prefs = {'download.default_directory': self.crypto_market_cap_data_path}
        chrome_options.add_experimental_option('prefs', prefs)
        browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

        browser.get(self.coingecko_ranking_url)
        tbody = browser.find_element(By.TAG_NAME, 'tbody')
        rows = tbody.find_elements(By.TAG_NAME, 'tr')

        rank_page_handle = browser.current_window_handle

        for row in rows:
            url_td = row.find_elements(By.TAG_NAME, 'td')[1]
            div1 = url_td.find_element(By.CSS_SELECTOR, 'div.tw-flex')
            div2 = div1.find_elements(By.TAG_NAME, 'div')[1]
            a = div2.find_element(By.TAG_NAME, 'a')
            url = a.get_attribute('href') + '/historical_data'

            browser.switch_to.new_window('tab')
            browser.get(url)

            div1 = browser.find_element(By.CSS_SELECTOR, 'div.card-body')
            div2 = div1.find_element(By.CSS_SELECTOR, 'div.card-block')
            div3 = div2.find_element(By.CSS_SELECTOR, 'div.tw-flex.tw-justify-between')
            div4 = div3.find_element(By.CSS_SELECTOR, 'div.dropdown.tw-mt-4')
            menu = div4.find_element(By.CSS_SELECTOR, 'ul.dropdown-menu')
            elements = menu.find_elements(By.CSS_SELECTOR, 'a.dropdown-item')
            for e in elements:
                url = e.get_attribute('href')
                if url.endswith('.csv'):
                    browser.get(url)

            browser.close()
            browser.switch_to.window(rank_page_handle)

        browser.quit()

    def get_tickers_from_dir(self):
        tickers = []
        for filename in listdir(self.crypto_market_cap_data_path):
            tickers.append(filename.split('-')[0] + 'USDT')
        return tickers


def main():
    engine = crypto_data_engine()
    engine.get_historical_data_by_range(engine.get_tickers_from_dir(), 1614556800, 1654473600, '1m')
    # KSMUSDT completed
    # engine.crawl_historical_market_cap_by_range_coingecko()


if __name__ == '__main__':
    main()
