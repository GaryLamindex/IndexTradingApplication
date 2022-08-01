import os
import sys
import pathlib
import datetime as dt
from csv import writer
from urllib.request import urlopen
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from zipfile import ZipFile, BadZipFile
from pycoingecko import CoinGeckoAPI
from ib_insync import *
import yfinance as yf
import math
import pandas as pd
from failure_handler import connection_handler, connect_tws


class grab_stock_data_engine:
    def __init__(self, ib_instance=None, update_list=False):
        # Ignore ib_instance parameter if only daily data is required
        self.ticker_name_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.resolve()) + '/etf_list'
        # If update_list or ticker_name.csv does not exist, create ticker_name.csv
        if not os.path.isdir(self.ticker_name_path):
            os.makedirs(self.ticker_name_path)
        file_exists = os.path.exists(f"{self.ticker_name_path}/ticker_name.csv")
        if (not file_exists) or update_list:
            url = "http://www.lazyportfolioetf.com/allocation/"
            page = urlopen(url)
            html_bytes = page.read()
            html = html_bytes.decode("utf-8")
            soup = BeautifulSoup(html, "lxml")
            portfolio_link = False
            links = list()
            for link in soup.find_all('a', href=True):
                if link['href'] == "http://www.lazyportfolioetf.com/allocation/10-year-treasury/":
                    portfolio_link = True
                if link['href'] == "https://twitter.com/intent/tweet?text=Lazy%20Portfolios:%20ETF%20Allocation&url=http://www.lazyportfolioetf.com/allocation":
                    portfolio_link = False
                if portfolio_link:
                    links.append(link['href'])
            links = list(dict.fromkeys(links))
            df = pd.DataFrame(columns=['Ticker'])
            for x in range(0, len(links)):
                soup1 = BeautifulSoup(requests.get(links[x]).text, "lxml")
                table = soup1.find('table',
                                   class_='w3-table table-padding-small w3-small font-family-arial table-valign-middle')
                for row in table.tbody.find_all('tr'):
                    columns = row.find_all('td')
                    if columns != []:
                        ticker = columns[2].b.contents[0]
                        df = pd.concat([df, pd.DataFrame.from_records([{'Ticker': ticker}])])
            df = df.drop_duplicates()
            df.to_csv(f"{self.ticker_name_path}/ticker_name.csv", index=False)
        # yfinance (daily) variables
        self.daily_ticker_data_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.resolve()) + '/ticker_data/one_day'
        # ib (minute) variable
        self.ib_instance = ib_instance
        if ib_instance is not None:
            self.ib_instance.reqMarketDataType(marketDataType=1)  # require live data
        # self.output_filepath = str(pathlib.Path(__file__).parent.parent.parent.resolve()) + f"/his_data/one_min"
        self.min_ticker_data_path = str(
            pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + "/ticker_data/one_min"
        self.grab_data_retry_attempt = 0

    def get_ticker_list(self):
        return pd.read_csv(self.ticker_name_path, header=0, names=['Ticker'])

    # yfinance (daily) methods
    def get_daily_data_by_period_helper(self, ticker, period):
        btc = yf.Ticker(ticker)
        hist = btc.history(period=period)
        return hist

    def get_daily_data_by_period(self, period, tickers=None):
        if not os.path.isdir(self.daily_ticker_data_path):
            os.makedirs(self.daily_ticker_data_path)
        if tickers is None:
            tickers = pd.read_csv(f"{self.ticker_name_path}/ticker_name.csv")['Ticker']
        if type(tickers) is str:
            tickers = [tickers]
        for ticker in tickers:
            df = self.get_daily_data_by_period_helper(ticker, period)
            index_list = df.index.tolist()
            timestamp = list()
            for x in range(len(index_list)):
                timestamp.append(int(index_list[x].timestamp()))
            df['timestamp'] = timestamp
            df = df.rename(columns={'Open': 'open'})
            df.to_csv(f"{self.daily_ticker_data_path}/{ticker}.csv", index=True, header=True)
            if not df.empty:
                print(f"Successfully download {ticker}.csv")
            else:
                print(f"Failed downloading {ticker}.csv")

    # Notice that data returned by yf.download is different from yf.Ticker().history, may have to solve later
    def get_daily_data_by_range_helper(self, ticker, start_timestamp, end_timestamp):
        start_date, end_date = start_timestamp.date() + dt.timedelta(days=1), end_timestamp.date() + dt.timedelta(days=1)
        hist = yf.download(ticker, start=start_date, end=end_date)
        return hist

    def get_daily_data_by_range(self, start_timestamp, end_timestamp, tickers=None):
        if not os.path.isdir(self.daily_ticker_data_path):
            os.makedirs(self.daily_ticker_data_path)
        if tickers is None:
            tickers = pd.read_csv(f"{self.ticker_name_path}/ticker_name.csv")['Ticker']
        if type(tickers) is str:
            tickers = [tickers]
        for ticker in tickers:
            df = self.get_daily_data_by_range_helper(self, ticker, start_timestamp, end_timestamp)
            index_list = df.index.tolist()
            timestamp = list()
            for x in range(len(index_list)):
                timestamp.append(int(index_list[x].timestamp()))
            df['timestamp'] = timestamp
            df = df.rename(columns={'Open': 'open'})
            df.to_csv(f"{self.daily_ticker_data_path}/{ticker}.csv", index=True, header=True)
            if not df.empty:
                print(f"Successfully downloaded {ticker}.csv")
            else:
                print(f"Failed downloading {ticker}.csv")

    def get_missing_daily_data(self, tickers=None):
        if type(tickers) is str:
            tickers = [tickers]
        if tickers is None:
            tickers = pd.read_csv(f"{self.ticker_name_path}/ticker_name.csv")['Ticker']
        today = dt.date.today()
        for ticker in tickers:
            if ticker == "^BTC":
                continue
            if not os.path.exists(f"{self.daily_ticker_data_path}/{ticker}.csv"):
                self.get_daily_data_by_period(period='max', tickers=ticker)
                continue
            else:
                existing_data = pd.read_csv(f"{self.daily_ticker_data_path}/{ticker}.csv",
                                            index_col='Date',
                                            parse_dates=True)
                last_update = existing_data.index[-1].date()
                days_passed = (today - last_update).days
            if days_passed < 1:
                missing_data = self.get_daily_data_by_period_helper(period='1d', ticker=ticker)
            elif days_passed < 5:
                missing_data = self.get_daily_data_by_period_helper(period='5d', ticker=ticker)
            elif days_passed < 30:
                missing_data = self.get_daily_data_by_period_helper(period='1mo', ticker=ticker)
            else:
                missing_data = self.get_daily_data_by_period_helper(period='max', ticker=ticker)
            index_list = missing_data.index.tolist()
            timestamp = list()
            for x in range(len(index_list)):
                timestamp.append(int(index_list[x].timestamp()))
            missing_data['timestamp'] = timestamp
            missing_data = missing_data.rename(columns={'Open': 'open'})
            updated_data = pd.concat([existing_data, missing_data]).reset_index().drop_duplicates(subset='Date', keep='last').set_index('Date')
            updated_data.to_csv(f"{self.daily_ticker_data_path}/{ticker}.csv", index=True, header=True)
            if not updated_data.empty:
                print(f"Successfully updated {ticker}.csv")
            else:
                print(f"Failed updating {ticker}.csv")


    # ib (minute) functions

    # helper functions

    # get data by passing in the start timestamp and the end timestamp
    # there may be request limit for this function, while the limit is set by TWS
    """just the helper function, NOT called directly"""

    def get_min_historical_data_helper(self, ticker, end_timestamp, duration, bar_size, regular_trading_hour):
        """
        end_timestamp: an unix timestamp
        duration: the length of data to be retrieved (couting bask starting fron the end_date), Examples: ‘60 S’, ‘30 D’, ‘13 W’, ‘6 M’, ‘10 Y’
        bar_size: the timelapse between 2 data, must be one of: ‘1 secs’, ‘5 secs’, ‘10 secs’ 15 secs’, ‘30 secs’, ‘1 min’, ‘2 mins’, ‘3 mins’, ‘5 mins’, ‘10 mins’, ‘15 mins’, ‘20 mins’, ‘30 mins’, ‘1 hour’, ‘2 hours’, ‘3 hours’, ‘4 hours’, ‘8 hours’, ‘1 day’, ‘1 week’, ‘1 month’.
        regular_trading_hour: a switch (boolean value) to allow user to choose whether to get data in the regular trading hour (True) or not (False)
        """
        end_date = dt.datetime.fromtimestamp(end_timestamp, tz=dt.timezone(dt.timedelta(hours=8)))
        contract = Stock(ticker, 'SMART', "USD")  # create the contract in the dictionary
        self.ib_instance.qualifyContracts(contract)  # qualify the contract
        data = self.ib_instance.reqHistoricalData(contract, end_date, durationStr=duration, barSizeSetting=bar_size,
                                                  whatToShow="TRADES", useRTH=regular_trading_hour)
        self.ib_instance.sleep(1)  # allows the data to fill gradually

        return data

    def get_min_data_by_range(self, start_timestamp, end_timestamp, ticker, bar_size, regular_trading_hour, changed):
        """
        It is a function only used by get_min_historical_data_by_range. It appends the given range of timestamps of data to
        the existent ticker file.
        """
        current_end_timestamp = end_timestamp
        connect_tws(self.ib_instance)
        while current_end_timestamp > start_timestamp:
            current_data = self.get_min_historical_data_helper(ticker, current_end_timestamp, '3 W', bar_size,
                                                           regular_trading_hour)

            if len(current_data) == 0:
                current_data = self.get_min_historical_data_helper(ticker, current_end_timestamp, '1 D', bar_size,
                                                               regular_trading_hour)
            if len(current_data) == 0:
                if self.grab_data_retry_attempt < 5:
                    self.grab_data_retry_attempt = self.grab_data_retry_attempt + 1
                    continue
                else:
                    self.grab_data_retry_attempt = 0
                    break
            front_timestamp = current_data[0].date.timestamp()
            current_data_df = util.df(current_data)
            current_data_df['timestamp'] = current_data_df[['date']].apply(
                lambda x: x[0].replace(tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp(), axis=1).astype(int)
            self.ib_instance.sleep(0)
            current_data_df.to_csv(f"{self.min_ticker_data_path}/{ticker}.csv", mode='a', index=False,
                                   header=False)
            print(
                f"Appended three weeks data for {ticker}, from {int(front_timestamp)} to {int(current_end_timestamp)}")
            current_end_timestamp = front_timestamp
            changed = True
        if changed:
            old_df = pd.read_csv(f"{self.min_ticker_data_path}/{ticker}.csv")
            old_df = old_df.loc[old_df["timestamp"] >= start_timestamp]
            old_df = old_df.drop_duplicates().sort_values(by=['timestamp'])
            old_df.to_csv(f"{self.min_ticker_data_path}/{ticker}.csv", index=False, header=True)

    def write_df_to_csv(self, ticker, df):
        """
        algorithm:
        if file already exists:
            read the file -> old data
            delete the old file
        create a new file
        write the current data to the new file (on the top) with header
        write the old data if file already exist
        """
        file_exist = f"{ticker}.csv" in os.listdir(self.min_ticker_data_path)
        if file_exist:  # file already exist
            old_df = pd.read_csv(f"{self.min_ticker_data_path}/{ticker}.csv")
            try:
                os.remove(f"{self.min_ticker_data_path}/{ticker}.csv")
            except Exception as e:
                print(f"Some errors occur, error message: {e}")

        with open(f"{self.min_ticker_data_path}/{ticker}.csv", "a+", newline='') as f:
            df.to_csv(f, mode='a', index=False, header=True)  # write the current data with header
            if file_exist:
                old_df.to_csv(f, mode='a', index=False, header=False)  # write the old data

        print(f"[{dt.datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] Successfully appended {ticker}.csv")

    # main functions
    @connection_handler
    def get_ibkr_open_price(self, tickers):
        """
        Example:
        input:
        output:
        """
        connect_tws(self.ib_instance)

        ticker_info_dict = {}  # the dictionary to be returned
        contracts = {}  # a dictionary of the contract object
        for ticker in tickers:
            contracts[ticker] = Stock(ticker, "SMART", "USD")  # create the contract in the dictionary
            self.ib_instance.qualifyContracts(contracts[ticker])  # qualify the contract
            self.ib_instance.reqMktData(contracts[ticker], '', False, False)  # subsribe the market data of the contact
            self.ib_instance.sleep(0)  # update the ib instance
            raw_ticker_info = self.ib_instance.reqTickers(contracts[ticker])[0]
            self.ib_instance.sleep(1)  # allows the Tickers info to fill gradually

            # fetch until information is completed
            while math.isnan(raw_ticker_info.bid) or math.isnan(raw_ticker_info.bidSize) or math.isnan(
                    raw_ticker_info.ask) or math.isnan(raw_ticker_info.askSize) or math.isnan(
                raw_ticker_info.last) or math.isnan(raw_ticker_info.lastSize) or math.isnan(raw_ticker_info.volume):
                print("Information incomplete, data:", raw_ticker_info)
                print("Trying again...")
                contracts[ticker] = Stock(ticker, "SMART", "USD")  # create the contract in the dictionary
                self.ib_instance.qualifyContracts(contracts[ticker])  # qualify the contract
                self.ib_instance.reqMktData(contracts[ticker], '', False,
                                            False)  # subsribe the market data of the contact
                self.ib_instance.sleep(0)  # update the ib instance
                raw_ticker_info = self.ib_instance.reqTickers(contracts[ticker])[0]
                self.ib_instance.sleep(1)  # allows the Tickers info to fill gradually

            print(raw_ticker_info)

            ticker_info = {"timestamp": raw_ticker_info.time.timestamp(), "bid": raw_ticker_info.bid,
                           "bidSize": raw_ticker_info.bidSize, "ask": raw_ticker_info.ask,
                           "askSize": raw_ticker_info.askSize, "last": raw_ticker_info.last,
                           "lastSize": raw_ticker_info.lastSize, "volume": raw_ticker_info.volume,
                           "close": raw_ticker_info.close, "halted": raw_ticker_info.halted}
            ticker_info_dict[ticker] = ticker_info

        return ticker_info_dict



    """
    Details of the head timestamp for each stocks:
    QQQ: 1999-03-10 14:30:00
    SPY: 1993-01-29 09:00:00
    // 1993-04-01 00:00:00 - 733593600
    TQQQ: 2010-02-11 09:00:00
    // 2010-03-01 00:00:00 - 1267372800
    UPRO: 2009-06-26 08:00:00
    // 2009-07-01 00:00:00 - 1246377600
    """

    def get_first_row_of_data(self, ticker):
        filename = f'{self.min_ticker_data_path}/{ticker}.csv'
        file_exists = os.path.exists(filename)
        if file_exists:
            df = pd.read_csv(filename)
            return df.iloc[0]
        else:
            return None

    # write the historical data to the
    # the function is for fetching large dataframe, thus inside the loop will only fetch the data of one week once
    # due to TWS limitation, max. 2 tickers at a time !!!
    # e.g. {"QQQ":[{timestamp, ohlc},{timestamp, ohlc}],"SPY"[{timestamp, ohlc},{timestamp, ohlc}]...}
    @connection_handler
    def get_min_historical_data_by_range(self, ticker, start_timestamp, end_timestamp, bar_size, regular_trading_hour):
        """
        This function will modify the file to the given range of timestamps for the existing ticker data file without
        deleting the old data. For a non-existent ticker data file, this function will download the non-existent ticker
        data file to the given range of timestamps. In the end, if there is no more data within a given range of
        timestamps in IB, a warning will be issued twelve times. The user can ignore it once the system says the CSV file
        was successfully appended.
        """
        file_existed = False  # check whether there already exist ticker file before running the function
        empty_file = True   # check whether the ticker file is empty
        changed = False  # check whether the file has been changed
        file_exist = f"{ticker}.csv" in os.listdir(self.min_ticker_data_path)
        if file_exist:  # if file already exist, check which date does the file updated to
            file_existed = True
            check_df = pd.read_csv(f"{self.min_ticker_data_path}/{ticker}.csv")
            update_date = check_df["timestamp"].max()    # the file was updated this date
        current_end_timestamp = end_timestamp

        connect_tws(self.ib_instance)

        while current_end_timestamp > start_timestamp:
            current_data = self.get_min_historical_data_helper(ticker, current_end_timestamp, '3 W', bar_size,
                                                           regular_trading_hour)

            if len(current_data) == 0:
                current_data = self.get_min_historical_data_helper(ticker, current_end_timestamp, '1 D', bar_size,
                                                               regular_trading_hour)
            if len(current_data) == 0:
                if self.grab_data_retry_attempt <= 5:
                    self.grab_data_retry_attempt = self.grab_data_retry_attempt + 1
                    raise Exception
                else:
                    self.grab_data_retry_attempt = 0
                    return

            front_timestamp = current_data[0].date.timestamp()
            current_data_df = util.df(current_data)
            current_data_df['timestamp'] = current_data_df[['date']].apply(
                lambda x: x[0].replace(tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp(), axis=1).astype(int)
            if file_existed:    # if the file already existed before running the function
                if current_data_df["timestamp"].iloc[0] <= update_date and current_data_df["timestamp"].iloc[-1] != update_date:  # If the file will be updated to the given end timestamp after appending the current dataframe
                    current_data_df.to_csv(f"{self.min_ticker_data_path}/{ticker}.csv", mode='a', index=False,
                                           header=False)  # append current data to the old file
                    print(
                        f"Appended three weeks data for {ticker}, from {int(front_timestamp)} to {int(current_end_timestamp)}")
                    changed = True
                    break
                elif current_data_df["timestamp"].iloc[-1] == update_date:  # the file already updated to the given end timestamp
                    break
            elif empty_file:    # if the file does not exist
                current_data_df.to_csv(f"{self.min_ticker_data_path}/{ticker}.csv", mode='w', index=False,
                                       header=True)  # write the current data with header
                print(
                    f"Appended three weeks data for {ticker}, from {int(front_timestamp)} to {int(current_end_timestamp)}")
                current_end_timestamp = front_timestamp
                changed = True
                empty_file = False  # the file is not empty now
                continue
            # sleep(10) # wait to fetch another batch of data
            self.ib_instance.sleep(0)  # refresh the ib instance
            current_data_df.to_csv(f"{self.min_ticker_data_path}/{ticker}.csv", mode='a', index=False,
                                   header=False)  # append current data to the old file
            print(
                f"Appended three weeks data for {ticker}, from {int(front_timestamp)} to {int(current_end_timestamp)}")
            current_end_timestamp = front_timestamp
            changed = True

        old_df = pd.read_csv(f"{self.min_ticker_data_path}/{ticker}.csv")
        if changed:
            old_df = old_df.loc[old_df["timestamp"] >= start_timestamp]
            old_df = old_df.drop_duplicates().sort_values(by=['timestamp'])
            old_df.to_csv(f"{self.min_ticker_data_path}/{ticker}.csv", index=False, header=True)
        if old_df["timestamp"].iloc[0] != start_timestamp:  # if the file is not updated to the given start timestamp
            oldest_timestamp = old_df["timestamp"].iloc[0]
            print(f"start fetching data from {int(start_timestamp)} to {int(oldest_timestamp)}")
            if changed:
                self.get_min_data_by_range(start_timestamp, old_df["timestamp"].iloc[0], ticker, '1 min', False, True)
            else:  # if the file has not been changed
                self.get_min_data_by_range(start_timestamp, old_df["timestamp"].iloc[0], ticker, '1 min', False, False)
        print(f"[{dt.datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] Successfully appended {ticker}.csv")

    def get_sehk_historical_data_by_range(self, ticker, start_timestamp, end_timestamp,
                                          bar_size, regular_trading_hour):
        current_end_timestamp = end_timestamp
        contract = Stock(ticker, 'SEHK', 'HKD')
        self.ib_instance.qualifyContracts(contract)

        while current_end_timestamp > start_timestamp:
            end_date = dt.datetime.fromtimestamp(current_end_timestamp)
            current_data = self.ib_instance.reqHistoricalData(contract, end_date, whatToShow="TRADES",
                                                              durationStr='3 W',
                                                              barSizeSetting=bar_size, useRTH=regular_trading_hour)
            current_end_timestamp = current_data[0].date.timestamp()
            self.ib_instance.sleep(0)
            current_data_df = util.df(current_data)  # convert into df
            current_data_df['timestamp'] = current_data_df[['date']].apply(
                lambda x: x[0].replace(tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp(), axis=1).astype(int)

            # print(current_data_df)  # only for testing
            self.write_df_to_csv(ticker, current_data_df)

    def get_multiple_min_data_by_range(self, start_timestamp, end_timestamp, bar_size,
                                              regular_trading_hour, tickers=None):
        if tickers is None:
            tickers = pd.read_csv(f"{self.ticker_name_path}/ticker_name.csv")['Ticker']
        for ticker in tickers:
            self.get_min_historical_data_by_range(ticker, start_timestamp, end_timestamp, bar_size, regular_trading_hour)
            print("successfully written", ticker)

    def update_csv(self, old_csv, update_csv, sort_values_col):
        old_df = pd.read_csv(old_csv, index_col=[sort_values_col])
        new_df = pd.read_csv(update_csv, index_col=[sort_values_col])
        common_col = list(set(old_df.columns).intersection(set(new_df.columns)))
        common_col_df = pd.concat([old_df[common_col], new_df[common_col]]).drop_duplicates(keep=False)
        df2 = pd.concat([common_col_df, old_df[common_col]])
        df2 = df2[df2.duplicated(keep='last')]
        df = pd.concat([common_col_df, df2]).drop_duplicates(keep=False)
        rows = df.values.tolist()
        for y in common_col:
            new_df.drop(y, inplace=True, axis=1)
        with open(old_csv, 'a+', newline='') as f:
            append_writer = writer(f)
            append_writer.writerow(rows)


class grab_crypto_data_engine:
    def __init__(self):
        self.ticker_data_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.resolve()) + '/ticker_data/one_min'
        self.crypto_daily_data_path = str(pathlib.Path(__file__)
                                          .parent.parent.parent.parent.resolve()) + '/ticker_data/crypto_daily'
        self.binance_base_url = 'https://data.binance.vision'
        self.coingecko_ranking_url = 'https://www.coingecko.com/en/all-cryptocurrencies'
        self.coingecko_historical_data_url = f'https://www.coingecko.com/en/coins/cardano/historical_data?end_date={dt.date.today().strftime("%Y-%m-%d")}&start_date=2021-03-01#panel'
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

    # download data from binance but only one year is available
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

    # merge all binance data
    def get_historical_data_by_range(self, tickers, start_timestamp, end_timestamp, bar_size):
        if type(tickers) is str:
            tickers = [tickers]
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

    # crawl coingecko market_cap data with Selenium
    def crawl_historical_market_cap_by_range_coingecko(self):
        chrome_options = webdriver.ChromeOptions()
        prefs = {'download.default_directory': self.crypto_daily_data_path}
        chrome_options.add_experimental_option('prefs', prefs)
        browser = webdriver.Chrome(service=Service(webdriver.ChromeDriverManager().install()), options=chrome_options)

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

    # get tickers from reading the directory
    # the directory is grabbed on coingecko
    def get_tickers_from_dir(self):
        tickers = []
        is_start = False
        for filename in os.listdir(self.crypto_daily_data_path):
            if is_start:
                tickers.append(filename.split('-')[0] + 'USDT')
            if filename == 'ksm-usd-max.csv':
                is_start = True
        return tickers

    # it returns a dataframe after reading data from coingecko
    def get_crypto_daily_data(self, ticker):
        filename = f'{self.crypto_daily_data_path}/{ticker.lower()}-usd-max.csv'
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], format='%Y-%m-%d %H:%M:%S %Z')
            df.set_index('snapped_at', inplace=True)
            return df
        else:
            print('daily crypto data not found')
            return None

    # download yfinance data
    def get_yfinance_max_historical_data(self, ticker):
        btc = yf.Ticker(f'{ticker}-USD')
        hist = btc.history(period='max')
        return hist

    # download all crypto data with coingecko API
    def download_all_crypto_data(self):
        cg = CoinGeckoAPI()
        coins_list = cg.get_coins_list()
        for coin in coins_list:
            symbol = coin['symbol']
            df = self.get_yfinance_max_historical_data(symbol)
            if not df.empty:
                df.to_csv(f'{self.crypto_daily_data_path}/{symbol.upper()}.csv')

# For testing only
def main():
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=1)
    stock_engine = grab_stock_data_engine(ib_instance=ib)
    stock_engine.get_multiple_min_data_by_range(0, dt.datetime.now().timestamp(),  '1 min', False, tickers=None)


if __name__ == "__main__":
    main()

