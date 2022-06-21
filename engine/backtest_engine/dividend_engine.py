import pathlib
import os

import pandas as pd


class dividend_engine:
    full_dividend_df = None
    tickers = []
    filepath = ""
    dividend_dir = ""

    def __int__(self, tickers, start_timestamp, end_timestamp, ):
        self.filepath = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/ticker_data/dividends"
        self.tickers = tickers
        list_of_dividend = os.listdir(self.dividend_dir)
        for file in list_of_dividend:
            ticker_name = file.split("_")
            for ticker in self.tickers:
                if ticker_name[0] == ticker:
                    temp = pd.read_csv(f"{self.filepath}/{file}.csv")
                    pd.concat([self.full_dividend_df, temp])
                    break

    def input_ticker(self, ticker):
        self.tickers.append(ticker)
        list_of_dividend = os.listdir(self.dividend_dir)
        for file in list_of_dividend:
            ticker_name = file.split("_")
            if ticker_name[0] == ticker:
                temp = pd.read_csv(f"{self.filepath}/{file}.csv")
                temp["ticker"] = ticker_name
                self.full_dividend_df = pd.concat([self.full_dividend_df, temp])
                break

    def delete_ticker(self, ticker):
        self.tickers.remove(ticker)
        self.full_dividend_df = self.full_dividend_df[self.full_dividend_df.ticker != ticker]




    def check_div(self, timestamp):
        for ticker in self.tickers:
            return
