import pathlib
import os
import re
import pandas as pd
import  datetime as dt
import yfinance as yf
import numpy as np
from datetime import datetime


class dividend_engine:
    full_dividend_df = None
    tickers = []
    filepath = ""
    portfolio = []
    dividends_data_path = ""
    dividend_date = []
    def __init__(self, tickers):
        self.filepath = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/ticker_data/dividends"
        self.tickers = tickers
        self.dividends_data_path = str(
            pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + "/ticker_data/dividends"
        if not os.path.exists(self.dividends_data_path):
            os.mkdir(self.dividends_data_path)
        list_of_dividend = os.listdir(self.filepath)
        self.get_dividends(tickers, -1)
        for ticker in self.tickers:
            for file in list_of_dividend:
                ticker_name = file.split("_")
                if ticker_name[0] == ticker:
                    temp = pd.read_csv(f"{self.filepath}/{file}")
                    temp["ticker"] = ticker
                    temp_timestamps = temp["timestamp"].to_numpy()
                    for timestamp in temp_timestamps:
                        utc_time = dt.date.fromtimestamp(timestamp)
                        self.dividend_date.append(utc_time)
                    self.full_dividend_df = pd.concat([self.full_dividend_df, temp])
                    break

    def input_ticker(self, ticker):
        self.tickers.append(ticker)
        self.get_dividends(ticker, -1)
        list_of_dividend = os.listdir(self.filepath)
        for file in list_of_dividend:
            ticker_name = file.split("_")
            if ticker_name[0] == ticker:
                temp = pd.read_csv(f"{self.filepath}/{file}")
                temp["ticker"] = ticker
                self.full_dividend_df = pd.concat([self.full_dividend_df, temp])
                break

    def delete_ticker(self, ticker):
        self.tickers.remove(ticker)
        self.full_dividend_df = self.full_dividend_df[self.full_dividend_df["ticker"] != ticker]

    def check_div(self, timestamp):
        utc_date = dt.date.fromtimestamp(timestamp)
        # if timestamp+60 == 1282003200:
        #     a=1
        return utc_date in self.dividend_date

    def distribute_div(self, timestamp, portfolio):
        self.portfolio = portfolio
        total_dividend = 0
        for ticker in self.tickers:
            ticker_dividend_df = self.full_dividend_df[self.full_dividend_df["ticker"] == ticker]
            ticker_dividend_df = ticker_dividend_df[ticker_dividend_df["timestamp"] == (timestamp+60)]
            if ticker_dividend_df.empty:
                ticker_dividend = 0
            else:
                ticker_dividend = ticker_dividend_df["dividends"].item()
            for ticker_info in self.portfolio:
                if ticker_info.get('ticker') == ticker:
                    ticker_pos = ticker_info.get('position')
                    dividend = ticker_pos * ticker_dividend
                    total_dividend = total_dividend + dividend
                    break
        return total_dividend

    def get_dividends(self, tickers, expire_day=-1):
        for ticker in tickers:
            ticker = ticker.upper()
            ticker_obj = yf.Ticker(ticker)
            dividends = pd.DataFrame(ticker_obj.dividends)
            dirs = os.listdir(self.dividends_data_path)
            today = datetime.today().strftime('%Y/%m/%d')

            timestamps = []

            for index in dividends.index:
                timestamps.append(int(index.timestamp()))

            dividends['timestamp'] = timestamps

            today_dt = dt.datetime.now()
            dividends = dividends.rename({'Date': 'date', 'Dividends': 'dividends'}, axis=1)

            expired = True
            for file in dirs:
                if ticker == re.sub('[^A-Z]', '', file):  # if there exists the csv file of the ticker
                    download_date = datetime.fromtimestamp(int(re.search(r'\d+', file).group())).strftime('%Y/%m/%d')
                    if (datetime.strptime(today, '%Y/%m/%d') - datetime.strptime(download_date,
                                                                                 '%Y/%m/%d')).days > expire_day:
                        os.remove(os.path.join(self.dividends_data_path, file))  # if csv file is expired, delete it
                    else:
                        expired = False
                    break
            if expired:  # if csv file is expired or doesn't exist, download the new csv file
                dividends.to_csv(
                    f'{self.dividends_data_path}/{ticker}_{int(dt.datetime(today_dt.year, today_dt.month, today_dt.day, tzinfo=dt.timezone.utc).timestamp())}.csv')


def main():
# dividend_agent = dividend_engine(["AAPL"], 547689600, 1651795200,
#                                  [{'ticker': "AAPL", 'position': 88},{'ticker': "QQQ", 'position': 50}])
# dividend_agent.input_ticker("QQQ")
# dividend = dividend_agent.check_div(1541635200)
# print(dividend)
    etf_list_path = str(
        pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + '/etf_list/scraper.csv'
    etf_list = pd.read_csv(etf_list_path)
    etf_list = etf_list["Ticker"].values.tolist()
    etf_list = list(set(etf_list))
    engine = dividend_engine(etf_list)
    engine.get_dividends(etf_list, -1)

if __name__ == "__main__":
    main()
