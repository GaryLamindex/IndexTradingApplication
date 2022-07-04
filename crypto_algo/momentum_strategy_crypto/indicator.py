import pandas as pd


class Indicator:
    # df: the dataframe for storing the data you need
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.evaluated_dict = {}  # a boolean dictionary stores whether the field has evaluated before

    # periods: how long should the pct_change involves
    # col_name: the pct_change of the column name you need
    def get_pct_change(self, periods: int, col_name: str, timestamp: int):
        if col_name not in self.evaluated_dict.keys():
            self.df[f'pct_change_{col_name}'] = self.df[col_name].pct_change(periods=periods)
            self.evaluated_dict[col_name] = True
        return self.df.loc[self.df['timestamp'] == timestamp, f'pct_change_{col_name}']
