import csv
import time

import re

import numpy as np
from ib_insync import *
import time
import datetime
import datetime as dt
from datetime import datetime
import os
import math
import pandas as pd

import sys
import pathlib

from numpy.random._examples.cffi.extending import vals

from failure_handler import connection_handler, connect_tws

import yfinance as yf
from csv import writer

sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()))

"""note: ALL the returning datetime will be set according to the BASE (but WITHOUT timezone info), i.e., Hong Kong time in this case. Timestamp should be adjusted accordingly"""
"""all the time input and comparison must be done according to the BASE timezone, i.e., Hong Kong time"""


# # helper functions
# def timestamp_to_hk_dt_string(timestamp): # useless now
#     HK_datetime = dt.datetime.fromtimestamp(timestamp,tz=dt.timezone(dt.timedelta(hours=8)))
#     year = str(HK_datetime.year)
#     month = str(HK_datetime.month) if HK_datetime.month >= 10 else "0" + str(HK_datetime.month)
#     day = str(HK_datetime.day) if HK_datetime.day >= 10 else "0" + str(HK_datetime.day)
#     hour = str(HK_datetime.hour) if HK_datetime.hour >= 10 else "0" + str(HK_datetime.hour)
#     minute = str(HK_datetime.minute) if HK_datetime.minute >= 10 else "0" + str(HK_datetime.minute)
#     second = str(HK_datetime.second) if HK_datetime.second >= 10 else "0" + str(HK_datetime.second)
#     HK_date_string = f"{year}{month}{day} {hour}:{minute}:{second}"
#     return HK_date_string

def raw_data_to_df(raw_data):
    df = util.df(raw_data)
    print(df)
    return df


def combine_csv(filepath, csv_list, output_name):
    df_list = []
    for csv in csv_list:
        df = pd.read_csv(f"{filepath}/{csv}")
        df_list.append(df)

    full_df = pd.concat(df_list)

    # remove the output file if exist
    file_exist = f"{output_name}.csv" in os.listdir(filepath)
    if file_exist:
        try:
            os.remove(f"{filepath}/{output_name}.csv")
        except Exception as e:
            print(f"Some errors occur, error message: {e}")

    # write the entire df to the output file
    with open(f"{filepath}/{output_name}.csv", "a+", newline='') as f:
        full_df.to_csv(f, mode='a', index=False, header=True)  # write the current data with header

    print("Successfully combined csv files")


class ibkr_stock_data_io_engine:
    ib_instance = None
    ticker_data_path = ""

    def __init__(self, ib_instance=None):
        self.ib_instance = ib_instance
        if ib_instance is not None:
            self.ib_instance.reqMarketDataType(marketDataType=1)  # require live data
        # self.output_filepath = str(pathlib.Path(__file__).parent.parent.parent.resolve()) + f"/his_data/one_min"
        self.ticker_data_path = str(
            pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + "/ticker_data/one_min"

        self.etf_list_path = str(
            pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + '/etf_list/etf_list.csv'

        self.grab_data_retry_attempt = 0

        # self.output_filepath = "C:/Users/85266/OneDrive/Documents"

    # return a dictionary of the latest price of the stock

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
                print("Information incompleted, data:", raw_ticker_info)
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

    # get data by passing tin the start timestamp and the end timestamp
    # there may be request limit for this function, while the limit if set by TWS
    """just the helper function, NOT called directly"""

    def get_historical_data_helper(self, ticker, end_timestamp, duration, bar_size, regular_trading_hour):
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
        filename = f'{self.ticker_data_path}/{ticker}.csv'
        file_exists = os.path.exists(filename)
        if file_exists:
            df = pd.read_csv(filename)
            return df.iloc[0]
        else:
            return None

    # write the historical data to the
    # the function is for fetching large dataframe, thus inside the loop will only fetch the data of one week once
    # due to TWS limitataion, max. 2 tickers at a time !!!
    # e.g. {"QQQ":[{timestamp, ohlc},{timestamp, ohlc}],"SPY"[{timestamp, ohlc},{timestamp, ohlc}]...}
    @connection_handler
    def get_historical_data_by_range(self, ticker, start_timestamp, end_timestamp, bar_size, regular_trading_hour):
        """
        This function will modify the file to the given range of timestamps for the existing ticker data file without
        deleting the old data. For a non-existent ticker data file, this function will download the non-existent ticker
        data file to the given range of timestamps. In the end, if there is no more data within a given range of
        timestamps in IB, a warning will be issued twelve times. The user can ignore it once the system says the CSV file
        was successfully appended.
        """
        file_existed = False  # check whether there already exist ticker file before running the function
        empty_file = True  # check whether the ticker file is empty
        changed = False  # check whether the file has been changed
        file_exist = f"{ticker}.csv" in os.listdir(self.ticker_data_path)
        if file_exist:  # if file already exist, check which date does the file updated to
            file_existed = True
            check_df = pd.read_csv(f"{self.ticker_data_path}/{ticker}.csv")
            update_date = check_df["timestamp"].max()  # the file was updated this date
        current_end_timestamp = end_timestamp

        connect_tws(self.ib_instance)

        while current_end_timestamp > start_timestamp:
            current_data = self.get_historical_data_helper(ticker, current_end_timestamp, '3 W', bar_size,
                                                           regular_trading_hour)

            if len(current_data) == 0:
                current_data = self.get_historical_data_helper(ticker, current_end_timestamp, '1 D', bar_size,
                                                               regular_trading_hour)
            if len(current_data) == 0:
                if self.grab_data_retry_attempt <= 5:
                    self.grab_data_retry_attempt = self.grab_data_retry_attempt + 1
                    continue
                else:
                    self.grab_data_retry_attempt = 0
                    break

            front_timestamp = current_data[0].date.timestamp()
            current_data_df = util.df(current_data)
            current_data_df['timestamp'] = current_data_df[['date']].apply(
                lambda x: x[0].replace(tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp(), axis=1).astype(int)
            if file_existed:  # if the file already existed before running the function
                if current_data_df["timestamp"].iloc[0] <= update_date and current_data_df["timestamp"].iloc[
                    -1] != update_date:  # If the file will be updated to the given end timestamp after appending the current dataframe
                    current_data_df.to_csv(f"{self.ticker_data_path}/{ticker}.csv", mode='a', index=False,
                                           header=False)  # append current data to the old file
                    print(
                        f"Appended three weeks data for {ticker}, from {int(front_timestamp)} to {int(current_end_timestamp)}")
                    changed = True
                    break
                elif current_data_df["timestamp"].iloc[
                    -1] == update_date:  # the file already updated to the given end timestamp
                    break
            elif empty_file:  # if the file does not exist
                current_data_df.to_csv(f"{self.ticker_data_path}/{ticker}.csv", mode='w', index=False,
                                       header=True)  # write the current data with header
                print(
                    f"Appended three weeks data for {ticker}, from {int(front_timestamp)} to {int(current_end_timestamp)}")
                current_end_timestamp = front_timestamp
                changed = True
                empty_file = False  # the file is not empty now
                continue
            # sleep(10) # wait to fetch another batch of data
            self.ib_instance.sleep(0)  # refresh the ib instance
            current_data_df.to_csv(f"{self.ticker_data_path}/{ticker}.csv", mode='a', index=False,
                                   header=False)  # append current data to the old file
            print(
                f"Appended three weeks data for {ticker}, from {int(front_timestamp)} to {int(current_end_timestamp)}")
            current_end_timestamp = front_timestamp
            changed = True

        old_df = pd.read_csv(f"{self.ticker_data_path}/{ticker}.csv")
        if changed:
            old_df = old_df.loc[old_df["timestamp"] >= start_timestamp]
            old_df = old_df.drop_duplicates().sort_values(by=['timestamp'])
            old_df.to_csv(f"{self.ticker_data_path}/{ticker}.csv", index=False, header=True)
        if old_df["timestamp"].iloc[0] != start_timestamp:  # if the file is not updated to the given start timestamp
            oldest_timestamp = old_df["timestamp"].iloc[0]
            if "check" in old_df and old_df.loc[0, "check"] == "True": # if there doesn't exist any older data, return the function
                print(f"[{dt.datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] Successfully appended {ticker}.csv")
                return
            print(f"start fetching data from {int(start_timestamp)} to {int(oldest_timestamp)}")
            self.get_data_by_range(start_timestamp, old_df["timestamp"].iloc[0], ticker, '1 min', False)
        print(f"[{dt.datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] Successfully appended {ticker}.csv")

    def get_data_by_range(self, start_timestamp, end_timestamp, ticker, bar_size, regular_trading_hour):
        """
        It is a function only used by get_historical_data_by_range. It appends the given range of timestamps of data to
        the existent ticker file.
        """
        current_end_timestamp = end_timestamp
        connect_tws(self.ib_instance)
        while current_end_timestamp > start_timestamp:
            current_data = self.get_historical_data_helper(ticker, current_end_timestamp, '3 W', bar_size,
                                                           regular_trading_hour)

            if len(current_data) == 0:
                current_data = self.get_historical_data_helper(ticker, current_end_timestamp, '1 D', bar_size,
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
            current_data_df.to_csv(f"{self.ticker_data_path}/{ticker}.csv", mode='a', index=False,
                                   header=False)
            print(
                f"Appended three weeks data for {ticker}, from {int(front_timestamp)} to {int(current_end_timestamp)}")
            current_end_timestamp = front_timestamp
        old_df = pd.read_csv(f"{self.ticker_data_path}/{ticker}.csv")
        if "check" not in old_df:
            old_df = old_df.loc[old_df["timestamp"] >= start_timestamp]
            old_df = old_df.drop_duplicates().sort_values(by=['timestamp'])
            old_df["check"] = " "
            old_df.loc[0, "check"] = "True"
            old_df.to_csv(f"{self.ticker_data_path}/{ticker}.csv", index=False, header=True)

    def get_etf_list(self):
        return pd.read_csv(self.etf_list_path, header=0, names=['Ticker'])

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

    def write_df_to_csv(self, ticker, df):
        """
        algoithm:
        if file already exists:
            read the file -> old data
            delete the old file
        create a new file
        write the current data to the new file (on the top) with header 
        write the old data if file already exist
        """
        file_exist = f"{ticker}.csv" in os.listdir(self.ticker_data_path)
        if file_exist:  # file already exist
            old_df = pd.read_csv(f"{self.ticker_data_path}/{ticker}.csv")
            try:
                os.remove(f"{self.ticker_data_path}/{ticker}.csv")
            except Exception as e:
                print(f"Some errors occur, error message: {e}")

        with open(f"{self.ticker_data_path}/{ticker}.csv", "a+", newline='') as f:
            df.to_csv(f, mode='a', index=False, header=True)  # write the current data with header
            if file_exist:
                old_df.to_csv(f, mode='a', index=False, header=False)  # write the old data

        print(f"[{dt.datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] Successfully appended {ticker}.csv")

    def get_multiple_historical_data_by_range(self, tickers, start_timestamp, end_timestamp, bar_size,
                                              regular_trading_hour):
        for ticker in tickers:
            self.get_historical_data_by_range(ticker, start_timestamp, end_timestamp, bar_size, regular_trading_hour)
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


def main():
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=1)
    # contracts = [Stock(ticker,"SMART","USD") for ticker in tickers]
    engine = ibkr_stock_data_io_engine(ib)
    engine.get_dividends(['qqq', 'spy', 'aapl', 'msft'])

    # print(engine.get_ibkr_open_price("QQQ"))
    # for contract in contracts:
    #     ib.qualifyContracts(contract)
    #     ib.reqMktData(contract,'',False,False)
    # ib.sleep(1)

    # df = pd.DataFrame(
    #     index=[c.symbol for c in contracts],
    #     columns=['bid_size', 'bid_price', 'ask_price', 'ask_size', 'last_price', 'last_size', 'high', 'low', 'volume', 'close_price', 'last_timestamp'])
    # def onPendingTickers(tickers):
    #     for t in tickers:
    #         df.loc[t.contract.symbol] = (
    #             t.bidSize, t.bid, t.ask, t.askSize, t.last, t.lastSize, t.high, t.low, t.volume, t.close, t.time)
    #         clear_output(wait=True)
    #     print(df)
    # ib.pendingTickersEvent += onPendingTickers
    # ib.sleep(1000)
    # ib.pendingTickersEvent -= onPendingTickers
    # leave the data-end blank for ADJUSTED_LAST
    # bars = ib.reqHistoricalData(contracts[0],"",durationStr="1 Y",barSizeSetting="1 day",whatToShow="ADJUSTED_LAST",useRTH=False)
    # print(bars[0].date.utc )
    # df = util.df(bars)
    # print(df)
    # bars = ib.reqHistoricalData(contracts[0],"20220119 22:00:00",durationStr="1 D",barSizeSetting="15 mins",whatToShow="MIDPOINT",useRTH=True)
    # ib.sleep(0)
    # print(bars)
    # df = util.df(bars)
    # print(df)
    # while True:
    #     print(engine.get_ibkr_open_price(["QQQ","SPY"]))
    #     sleep(10)

    # dict[tickers[0]][0].date
    # print(engine.get_historical_data("SPY","20220118 00:00:00","6 D","1 min",False))
    # ticker = ib.reqTickers(contracts[0])
    # ib.sleep(1)
    # print(ticker[0].time)


if __name__ == "__main__":
    main()
