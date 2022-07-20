import pandas as pd


class Indicator:
    # df: the dataframe for storing the data you need
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.evaluated_dict = {}  # a boolean dictionary stores whether the field has evaluated before

    # periods: how long should the pct_change involves
    # col_name: the pct_change of the column name you need
    def get_pct_change(self, col_name: str, timestamp: int, periods: int):
        temp2 = f'pct_change{periods}_{col_name}'
        if col_name not in self.evaluated_dict.keys():
            self.df[temp2] = self.df[col_name].pct_change(periods=periods)
            self.evaluated_dict[temp2] = True
        temp = self.df.loc[self.df['timestamp'] == timestamp, temp2]

        if temp.shape[0] != 0:
            return self.df.loc[self.df['timestamp'] == timestamp, temp2].item()
        return None

    def get_high(self, col_name: str, timestamp: int):
        temp_col = f'high_{col_name}'
        if col_name not in self.evaluated_dict.keys():
            self.evaluated_dict[temp_col] = True
            self.df[temp_col] = self.df[col_name].expanding().max()
        temp = self.df.loc[self.df['timestamp'] == timestamp, temp_col]

        if temp.shape[0] != 0:
            return self.df.loc[self.df['timestamp'] == timestamp, temp_col].item()
        return None
