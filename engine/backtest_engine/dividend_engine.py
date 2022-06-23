import pathlib
import os

import pandas as pd


class dividend_engine:
    full_dividend_df = None
    tickers = []
    filepath = ""
    portfolio = []

    def __init__(self, tickers, start_timestamp, end_timestamp, portfolio):
        self.filepath = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/ticker_data/dividends"
        self.tickers = tickers
        self.portfolio = portfolio
        list_of_dividend = os.listdir(self.filepath)
        for file in list_of_dividend:
            ticker_name = file.split("_")
            for ticker in self.tickers:
                if ticker_name[0] == ticker:
                    temp = pd.read_csv(f"{self.filepath}/{file}")
                    temp["ticker"] = ticker
                    self.full_dividend_df = pd.concat([self.full_dividend_df, temp])
                    break

    def input_ticker(self, ticker):
        self.tickers.append(ticker)
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
        total_dividend = 0
        for ticker in self.tickers:
            ticker_dividend_df = self.full_dividend_df[self.full_dividend_df["ticker"] == ticker]
            ticker_dividend_df = ticker_dividend_df[ticker_dividend_df["timestamp"] == timestamp]
            ticker_dividend = ticker_dividend_df["dividends"].item()
            for ticker_info in self.portfolio:
                if ticker_info.get('ticker') == ticker:
                    ticker_pos = ticker_info.get('position')
                    dividend = ticker_pos * ticker_dividend
                    total_dividend = total_dividend + dividend
                    break
        return total_dividend


def main():
    dividend_agent = dividend_engine(["AAPL"], 547689600, 1651795200,
                                     [{'ticker': "AAPL", 'position': 88},{'ticker': "QQQ", 'position': 50}])
    dividend_agent.input_ticker("QQQ")
    dividend = dividend_agent.check_div(1541635200)
    print(dividend)


if __name__ == "__main__":
    main()
