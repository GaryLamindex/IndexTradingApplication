import pathlib
import os

import pandas as pd


class dividend_engine:
    full_dividend_df = None
    ticker = ""
    filepath = ""
    dividend_dir = ""

    def __int__(self, ticker, start_timestamp, end_timestamp, ):
        self.filepath = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/ticker_data/dividends"
        self.ticker = ticker
        list_of_dividend = os.listdir(self.dividend_dir)
        for file in list_of_dividend:
            ticker_name = file.split("_")
            if ticker_name[0] == ticker:
                self.dividend_df = pd.read_csv(f"{self.filepath}/{file}.csv")

