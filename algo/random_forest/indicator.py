import numpy as np
import pandas as pd
import pandas_ta as ta
import quandl
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor

class Indicator:
    def __init__(self, indices, rate):
        self.daily_indices = indices
        self.weekly_indices = indices.groupby(pd.Grouper(freq='W-Fri')).nth(-1)
        self.daily_return = indices['Close'].pct_change().rename("Return").iloc[1:]
        self.weekly_return = self.weekly_indices['Close'].pct_change().rename("Return").iloc[1:]
        self.rate = rate
        self.dataset = pd.DataFrame()
        self.last_dataset = np.array([])

    def get_samples(self):
        # return_day_1, return_day_5, return_day_20 = self.daily_indices.ta.percent_return(length=1).rename("Daily-Return-1"), \
        #                                             self.daily_indices.ta.percent_return(length=5).rename("Daily-Return-5"), \
        #                                             self.daily_indices.ta.percent_return(length=20).rename("Daily-Return-20")
        return_week_1, return_week_4, return_week_12 = self.weekly_indices.ta.percent_return(length=1).rename("Weekly-Return-1"), \
                                                       self.weekly_indices.ta.percent_return(length=4).rename("Weekly-Return-4"), \
                                                       self.weekly_indices.ta.percent_return(length=12).rename("Weekly-Return-12")
        # sma20 = ta.sma(self.daily_indices["Close"], length=20)
        # di20 = ((self.daily_indices['Close'] - sma20) / sma20).diff().rename("DI20")
        # rsi = ta.rsi(self.daily_indices['Close'], talib=False).rename("RSI")
        # macd = ta.macd(self.daily_indices['Close'], talib=False)['MACDh_12_26_9'].diff().rename("MACD")
        stoch = ta.stoch(self.daily_indices['High'], self.daily_indices['Low'], self.daily_indices['Close'])
        stoch_k, stoch_d = stoch['STOCHk_14_3_3'].rename("Stoch_k"), stoch['STOCHd_14_3_3'].rename("Stoch_d")
        stoch = (stoch_k - stoch_d).rename("Stoch")
        stoch_k_change, stoch_d_change = stoch_k.diff().rename("Stoch_k_change"), stoch_d.diff().rename("Stoch_d_change")
        # bollinger_df = ta.bbands(self.daily_indices['Close'], length=20, talib=False)
        # bollinger = (bollinger_df["BBU_20_2.0"] - self.weekly_indices['Close']) / (
        #             bollinger_df["BBU_20_2.0"] - bollinger_df["BBL_20_2.0"])
        # bollinger.name = "Bollinger"
        # weekly_rates = self.rate.groupby(pd.Grouper(freq='W-Fri')).nth(0).rename(
        #      columns={"Value": "Rate"}).pct_change()
        factors = [self.weekly_return,
                   # return_day_1, return_day_5, return_day_20,
                   return_week_4, # return_week_1, return_week_12,
                   stoch, stoch_k_change, stoch_d_change, # rsi, macd, bollinger,
                   # weekly_rates
                   ]
        self.dataset = pd.concat(factors, axis=1, join='inner')
        self.last_dataset = pd.concat(factors, axis=1, join='outer').stack().groupby(level=1).tail(1).values[1:]
        self.dataset['Return'] = self.dataset['Return'].shift(-1)
        self.dataset = self.dataset.dropna(how='any')
        a = 1

# For testing only
# def main():
#     indices = pd.read_csv("C:/Users/user/Documents/GitHub/ticker_data/one_day/SPY.csv",
#                           index_col='Date',
#                           usecols=['Date', 'High', 'Low', 'Close'],
#                           parse_dates=True)
#     quandl.ApiConfig.api_key = 'xdHPexePa-TVMtE5bMhA'
#     one_yr_rate = quandl.get('FRED/DGS1')
#     ten_yr_rate = quandl.get('FRED/DGS10')
#     rate = (100 + ten_yr_rate) / (100 + one_yr_rate)
#     indicator = Indicator(indices, ten_yr_rate)
#     indicator.get_samples()
#     regr = RandomForestRegressor(max_features=1/3,
#                                  min_samples_leaf=0.1,
#                                  random_state=23571113,
#                                  max_samples=0.7)
#     regr.fit(indicator.dataset.drop("Return", axis=1), indicator.dataset["Return"])
#     # possible_values = np.linspace(indicator.dataset["Rate"].min(), indicator.dataset["Rate"].max(), 1000).reshape(-1, 1)
#     # y = regr.predict(possible_values)
#     plt.scatter(indicator.dataset["DI20"], indicator.dataset["Return"])
#     plt.show()
#     a = 1
#
#
# if __name__ == '__main__':
#     main()
