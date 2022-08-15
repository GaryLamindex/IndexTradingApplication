import numpy as np
import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor

class Indicator:
    def __init__(self, indices):
        indices = pd.DataFrame(indices)
        self.daily_indices = indices
        self.weekly_indices = indices.groupby(pd.Grouper(freq='W-fri')).nth(-1)
        self.daily_return = indices.pct_change().rename(columns={"Close": "Return"}).iloc[1:]
        self.weekly_return = self.weekly_indices.pct_change().rename(columns={"Close": "Return"}).iloc[1:]
        self.dataset = pd.DataFrame()
        self.last_dataset = np.array([])

    def get_samples(self):
        return_day_1, return_day_5, return_day_20 = self.daily_indices.ta.percent_return(length=1).rename("Daily-Return-1"), \
                                                    self.daily_indices.ta.percent_return(length=5).rename("Daily-Return-5"), \
                                                    self.daily_indices.ta.percent_return(length=20).rename("Daily-Return-20")
        return_week_1, return_week_4, return_week_12 = self.weekly_indices.ta.percent_return(length=1).rename("Weekly-Return-1"), \
                                                       self.weekly_indices.ta.percent_return(length=4).rename("Weekly-Return-5"), \
                                                       self.weekly_indices.ta.percent_return(length=12).rename("Weekly-Return-20")
        rsi = ta.rsi(self.daily_indices['Close'], talib=False).rename("RSI")
        bollinger_df = ta.bbands(self.daily_indices['Close'], length=20, talib=False)
        bollinger = (bollinger_df["BBU_20_2.0"] - self.daily_indices['Close']) / (bollinger_df["BBU_20_2.0"] - bollinger_df["BBL_20_2.0"])
        bollinger.name = "Bollinger"
        self.dataset = pd.concat([self.weekly_return,
                                  return_day_1, return_day_5, return_day_20,
                                  return_week_1, return_week_4, return_week_12,
                                  rsi, bollinger], axis=1, join='inner')
        self.last_dataset = pd.concat([self.weekly_return,
                                       return_day_1, return_day_5, return_day_20,
                                       return_week_1, return_week_4, return_week_12,
                                       rsi, bollinger], axis=1, join='outer').stack().groupby(level=1).tail(1).values[1:]
        self.dataset['Return'] = self.dataset['Return'].shift(-1)
        self.dataset = self.dataset.dropna(how='any')
        a = 1

# For testing only
# def main():
#     indices = pd.read_csv("C:/Users/user/Documents/GitHub/ticker_data/one_day/SPY.csv",
#                           index_col='Date',
#                           usecols=['Date', 'Close'],
#                           parse_dates=True).iloc[:, 0]
#     indicator = Indicator(indices)
#     indicator.get_samples()
#     regr = RandomForestRegressor(max_features=1/3,
#                                  min_samples_leaf=0.005,
#                                  random_state=23571113,
#                                  max_samples=0.7)
#     regr.fit(indicator.dataset.drop("Return", axis=1), indicator.dataset["Return"])
#     print(regr.predict([indicator.dataset.drop("Return", axis=1).iloc[2]]))
#     a=1
#
#
# if __name__ == '__main__':
#     main()