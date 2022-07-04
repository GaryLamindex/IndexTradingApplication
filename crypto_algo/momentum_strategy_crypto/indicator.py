import pandas as pd


class Indicator:
    # df: dataframe
    def __init__(self, df):
        self.df = df

    # periods: how long should the pct_change involves
    # col_name: the pct_change of the column name you need
    def get_pct_change(self, periods, col_name):
        series = self.df[col_name].pct_change(periods=periods)
        return series
