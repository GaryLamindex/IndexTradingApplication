import pandas as pd


class Indicator:
    # df: the dataframe for storing the data you need
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.evaluated_dict = {}  # a boolean dictionary stores whether the field has evaluated before

    # periods: how long should the pct_change involves
    # col_name: the pct_change of the column name you need
    def get_pct_change(self, periods: int, col_name: str, timestamp: int):
        temp2 = f'pct_change{periods}_{col_name}'
        if col_name not in self.evaluated_dict.keys():
            self.df[temp2] = self.df[col_name].pct_change(periods=periods)
            self.evaluated_dict[temp2] = True
        temp = self.df.loc[self.df['timestamp'] == timestamp, temp2]

        if temp.shape[0] != 0:
            return self.df.loc[self.df['timestamp'] == timestamp, temp2].item()
        return None
