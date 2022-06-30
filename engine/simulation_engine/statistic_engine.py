import datetime

import pandas as pd
import numpy as np
import os
import sys
import datetime as dt
from matplotlib import pyplot as plt
from scipy import stats
from dateutil.relativedelta import relativedelta
from engine.simulation_engine import sim_data_io_engine
from object import backtest_acc_data

RISK_FREE_RATE = 0.0127
# equivalent rate for 60s
EQV_RISK_FREE_RATE = (1 + RISK_FREE_RATE) ** (60 / (365 * 24 * 60 * 60)) - 1


# a global function for risk free rate processing
# support 2 possible kw arguments: "timeframe" & "range"
# "timeframe" only support "1d", "1m", "6m", "1y" & "5y"
def eq_riskfree_rate(**kwargs):
    # check usage
    if len(kwargs) != 1 or (list(kwargs.keys())[0] != "timeframe" and list(kwargs.keys())[0] != "range"):
        sys.exit("Wrong parameter")
    if list(kwargs.keys())[0] == "timeframe":
        timeframe = list(kwargs.values())[0]
        if timeframe == "1d":
            return (1 + RISK_FREE_RATE) ** (1 / 365) - 1
        elif timeframe == "1m":
            return (1 + RISK_FREE_RATE) ** (1 / 12) - 1
        elif timeframe == "6m":
            return (1 + RISK_FREE_RATE) ** (1 / 2) - 1
        elif timeframe == "1y":
            return RISK_FREE_RATE
        elif timeframe == "5y":
            return (1 + RISK_FREE_RATE) ** 5 - 1
        else:
            sys.exit("Wrong parameter")
    elif list(kwargs.keys())[0] == "range":
        # parameter format example: ("2017-10-20","2017-11-11")
        dt1 = pd.to_datetime(range[0], format="%Y-%m-%d")
        dt2 = pd.to_datetime(range[1], format="%Y-%m-%d")

        no_of_days = (dt2 - dt1).days

        return (1 + RISK_FREE_RATE) ** (no_of_days / 365) - 1


# grab the data_engine for internal use
script_dir = os.path.dirname(__file__)
data_engine_dir = os.path.join(script_dir, '..', 'data_io_engine')
sys.path.append(data_engine_dir)


class statistic_engine:
    # private data members # modify later
    data_engine = None

    def __init__(self, data_engine):
        self.data_engine = data_engine

    """
    For the name of the columns, may need to change mannually (e.g., NetLiquidation those)
    """

    # functions about return
    def get_return_by_period(self, date, lookback_period, file_name):
        # there are totally 5 cases for the lookback period 
        # "1d", "1m", "6m", "1y", "3y" "5y"

        # get the sliced frame for the given lookback_period
        if lookback_period == "1d":
            data_period_df = self.data_engine.get_data_by_period(date, "1d", file_name)
        elif lookback_period == "1m":
            data_period_df = self.data_engine.get_data_by_period(date, "1m", file_name)
        elif lookback_period == "6m":
            data_period_df = self.data_engine.get_data_by_period(date, "6m", file_name)
        elif lookback_period == "1y":
            data_period_df = self.data_engine.get_data_by_period(date, "1y", file_name)
        elif lookback_period == "3y":
            data_period_df = self.data_engine.get_data_by_period(date, "3y", file_name)
        elif lookback_period == "5y":
            data_period_df = self.data_engine.get_data_by_period(date, "5y", file_name)

        starting_net_liquidity = \
            data_period_df.loc[data_period_df['timestamp'] == data_period_df['timestamp'].min()][
                'NetLiquidation'].values[0]
        ending_net_liquidity = \
            data_period_df.loc[data_period_df['timestamp'] == data_period_df['timestamp'].max()][
                'NetLiquidation'].values[0]

        return (ending_net_liquidity - starting_net_liquidity) / starting_net_liquidity

    def get_return_by_range(self, range, file_name):
        # get the sliced data for the given range
        print("range")
        print(range)
        range_df = self.data_engine.get_data_by_range(range, file_name)
        print("range_df")
        print(range_df)
        print()

        starting_net_liquidity = \
            range_df.loc[range_df['timestamp'] == range_df['timestamp'].min()]['NetLiquidation'].values[0]
        ending_net_liquidity = \
            range_df.loc[range_df['timestamp'] == range_df['timestamp'].max()]['NetLiquidation'].values[0]

        return (ending_net_liquidity - starting_net_liquidity) / starting_net_liquidity

    def get_return_inception(self, file_name):
        print("get_return_inception")
        inception_df = self.data_engine.get_full_df(file_name)

        starting_net_liquidity = \
            inception_df.loc[inception_df['timestamp'] == inception_df['timestamp'].min()]['NetLiquidation'].values[0]
        starting_ts = inception_df['timestamp'].min()
        ending_net_liquidity = \
            inception_df.loc[inception_df['timestamp'] == inception_df['timestamp'].max()]['NetLiquidation'].values[0]
        ending_ts = inception_df['timestamp'].max()
        print(f"starting_net_liquidity:{starting_net_liquidity}; ending_net_liquidity:{ending_net_liquidity}")
        no_of_years = (dt.datetime.fromtimestamp(ending_ts) - dt.datetime.fromtimestamp(starting_ts)).days / 365
        return ((ending_net_liquidity - starting_net_liquidity) / starting_net_liquidity, no_of_years)

    def get_return_ytd(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]
        return (self.get_return_by_range(range, file_name) , month/12)

    # return a dictionary of all return info (ytd, 1y, 3y, 5y and inception)
    def get_return_data(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"

        return_dict = {}
        return_inflation_adj_dict = {}
        return_dict["ytd"] , yr = self.get_return_ytd(file_name)
        return_inflation_adj_dict["ytd"] = 1 + return_dict.get('ytd') / (1.03 ** yr) - 1
        return_dict["1y"] = self.get_return_by_period(day_string, "1y", file_name)
        return_inflation_adj_dict["1y"] = 1 + return_dict.get('1y') / 1.03 - 1
        return_dict["3y"] = self.get_return_by_period(day_string, "3y", file_name)
        return_inflation_adj_dict["3y"] = 1 + return_dict.get('3y') / 1.03**3 - 1
        return_dict["5y"] = self.get_return_by_period(day_string, "5y", file_name)
        return_inflation_adj_dict["5y"] = 1 + return_dict.get('5y') / 1.03 ** 5 - 1
        return_dict["inception"] , yr = self.get_return_inception(file_name)
        return_inflation_adj_dict["inception"] = 1 + return_dict.get('inception') / 1.03 ** yr - 1



        return (return_dict, return_inflation_adj_dict)

    # simply use the discrete calculation for sharpe
    # annualize all the results
    def get_sharpe_by_period(self, date, lookback_period, file_name):
        # there are totally 5 cases for the lookback period 
        # "1d", "1m", "6m", "1y", "3y", "5y"

        # get the sliced frame for the given lookback_period
        if lookback_period == "1d":
            data_period_df = self.data_engine.get_data_by_period(date, "1d", file_name)
        elif lookback_period == "1m":
            data_period_df = self.data_engine.get_data_by_period(date, "1m", file_name)
        elif lookback_period == "6m":
            data_period_df = self.data_engine.get_data_by_period(date, "6m", file_name)
        elif lookback_period == "1y":
            data_period_df = self.data_engine.get_data_by_period(date, "1y", file_name)
        elif lookback_period == "3y":
            data_period_df = self.data_engine.get_data_by_period(date, "3y", file_name)
        elif lookback_period == "5y":
            data_period_df = self.data_engine.get_data_by_period(date, "5y", file_name)

        multiplier = {"1d": 365 ** 0.5, "1m": 12 ** 0.5, "6m": 2 ** 0.5, "1y": 1, "3y": 1 / (3 ** 0.5),
                      "5y": 1 / (5 ** 0.5)}

        ending_nlv = data_period_df['NetLiquidation']
        return_col = ending_nlv.pct_change().dropna()
        avg_period_return = np.array(return_col).mean()
        period_return_std = np.array(return_col).std()
        items = data_period_df.shape[0]
        return (items ** 0.5) * multiplier[lookback_period] * (
                avg_period_return - EQV_RISK_FREE_RATE) / period_return_std

    def get_sharpe_by_range(self, range, file_name):
        print("get_sharpe_by_range")
        range_df = self.data_engine.get_data_by_range(range, file_name)

        no_of_days = (pd.to_datetime(range[1], format="%Y-%m-%d") - pd.to_datetime(range[0],
                                                                                   format="%Y-%m-%d")).days + 1

        multiplier = (365 / no_of_days) ** 0.5

        ending_nlv = range_df['NetLiquidation']
        return_col = ending_nlv.pct_change().dropna()
        avg_period_return = np.array(return_col).mean()
        period_return_std = np.array(return_col).std()
        items = range_df.shape[0]
        return (items ** 0.5) * multiplier * (avg_period_return - EQV_RISK_FREE_RATE) / period_return_std

    def get_sharpe_inception(self, file_name):
        inception_df = self.data_engine.get_full_df(file_name)

        # the case of a specified spec
        if isinstance(self.data_engine, sim_data_io_engine.online_engine):
            date_column = inception_df['timestamp'].dt.date  # since online df returns timestamp data type
            no_of_days = (date_column.max() - date_column.min()).days + 1
        elif isinstance(self.data_engine, sim_data_io_engine.offline_engine):
            date_column = inception_df['timestamp']  # since offline df returns integer data type
            no_of_days = (date_column.max() - date_column.min()) / (24 * 60 * 60) + 1

        multiplier = (365 / no_of_days) ** 0.5

        ending_nlv = inception_df['NetLiquidation']
        return_col = ending_nlv.pct_change().dropna()
        avg_period_return = np.array(return_col).mean()
        period_return_std = np.array(return_col).std()
        items = inception_df.shape[0]
        return (items ** 0.5) * multiplier * (avg_period_return - EQV_RISK_FREE_RATE) / period_return_std

    def get_sharpe_ytd(self, file_name):
        print("get_sharpe_ytd")
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]
        return self.get_sharpe_by_range(range, file_name)

    # return a dictionary of all shpare info (ytd, 1y, 3y, 5y and inception)
    def get_sharpe_data(self, file_name):
        print(f"get_sharpe_data:{file_name}")
        sharpe_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"

        sharpe_dict["ytd"] = self.get_sharpe_ytd(file_name)
        sharpe_dict["1y"] = self.get_sharpe_by_period(day_string, "1y", file_name)
        sharpe_dict["3y"] = self.get_sharpe_by_period(day_string, "3y", file_name)
        sharpe_dict["5y"] = self.get_sharpe_by_period(day_string, "5y", file_name)
        sharpe_dict["inception"] = self.get_sharpe_inception(file_name)

        return sharpe_dict

    def get_max_drawdown_by_period(self, date, lookback_period, file_name):
        # there are totally 5 cases for the lookback period 
        # "1d", "1m", "6m", "1y", "3y" "5y"

        # get the sliced frame for the given lookback_period
        if lookback_period == "1d":
            data_period_df = self.data_engine.get_data_by_period(date, "1d", file_name)
        elif lookback_period == "1m":
            data_period_df = self.data_engine.get_data_by_period(date, "1m", file_name)
        elif lookback_period == "6m":
            data_period_df = self.data_engine.get_data_by_period(date, "6m", file_name)
        elif lookback_period == "1y":
            data_period_df = self.data_engine.get_data_by_period(date, "1y", file_name)
        elif lookback_period == "3y":
            data_period_df = self.data_engine.get_data_by_period(date, "3y", file_name)
        elif lookback_period == "5y":
            data_period_df = self.data_engine.get_data_by_period(date, "5y", file_name)

        rolling_max = data_period_df['NetLiquidation'].cummax()
        item_drawdown = data_period_df['NetLiquidation'] / rolling_max - 1
        return item_drawdown.min()

    def get_max_drawdown_by_range(self, range, file_name):
        # get the sliced data for the given range
        print("range")
        print(range)
        range_df = self.data_engine.get_data_by_range(range, file_name)
        print("range_df")
        print(range_df)
        print()

        rolling_max = range_df['NetLiquidation'].cummax()
        item_drawdown = range_df['NetLiquidation'] / rolling_max - 1
        return item_drawdown.min()

    def get_max_drawdown_inception(self, file_name):
        inception_df = self.data_engine.get_full_df(file_name)

        rolling_max = inception_df['NetLiquidation'].cummax()
        item_drawdown = inception_df['NetLiquidation'] / rolling_max - 1
        return item_drawdown.min()

    def get_max_drawdown_ytd(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]

        return self.get_max_drawdown_by_range(range, file_name)

    def get_max_drawdown_data(self, file_name):
        drawdown_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"

        drawdown_dict["ytd"] = self.get_max_drawdown_ytd(file_name)
        drawdown_dict["1y"] = self.get_max_drawdown_by_period(day_string, "1y", file_name)
        drawdown_dict["3y"] = self.get_max_drawdown_by_period(day_string, "3y", file_name)
        drawdown_dict["5y"] = self.get_max_drawdown_by_period(day_string, "5y", file_name)
        drawdown_dict["inception"] = self.get_max_drawdown_inception(file_name)

        return drawdown_dict

    def get_sortino_by_period(self, date, lookback_period, file_name):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)

        ending_nlv = data_period_df['NetLiquidation']
        return_col = ending_nlv.pct_change().dropna()
        avg_period_return = np.array(return_col).mean()

        return_col_arr = np.array(return_col)
        return_col_arr_neg = return_col_arr[return_col_arr < 0]
        return (avg_period_return - EQV_RISK_FREE_RATE) / return_col_arr_neg.std()

    def get_sortino_by_range(self, range, file_name):
        print("get_sortino_by_range")
        range_df = self.data_engine.get_data_by_range(range, file_name)

        ending_nlv = range_df['NetLiquidation']
        return_col = ending_nlv.pct_change().dropna()
        avg_period_return = np.array(return_col).mean()

        return_col_arr = np.array(return_col)
        return_col_arr_neg = return_col_arr[return_col_arr < 0]
        return (avg_period_return - EQV_RISK_FREE_RATE) / return_col_arr_neg.std()

    def get_sortino_inception(self, file_name):
        inception_df = self.data_engine.get_full_df(file_name)

        ending_nlv = inception_df['NetLiquidation']
        return_col = ending_nlv.pct_change().dropna()
        avg_period_return = np.array(return_col).mean()

        return_col_arr = np.array(return_col)
        return_col_arr_neg = return_col_arr[return_col_arr < 0]
        return (avg_period_return - EQV_RISK_FREE_RATE) / return_col_arr_neg.std()

    def get_sortino_ytd(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]

        return self.get_sortino_by_range(range, file_name)

    def get_sortino_data(self, file_name):
        sortino_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"

        sortino_dict["ytd"] = self.get_sortino_ytd(file_name)
        sortino_dict["1y"] = self.get_sortino_by_period(day_string, "1y", file_name)
        sortino_dict["3y"] = self.get_sortino_by_period(day_string, "3y", file_name)
        sortino_dict["5y"] = self.get_sortino_by_period(day_string, "5y", file_name)
        sortino_dict["inception"] = self.get_sortino_inception(file_name)

        return sortino_dict

    def get_alpha_by_period(self, date, lookback_period, file_name, marketCol):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)

        multiplier = {"1d": 60 * 24, "1m": 60 * 24 * 30, "6m": 60 * 24 * 30 * 6, "1y": 60 * 24 * 30 * 12,
                      "3y": 60 * 24 * 30 * 12 * 3,
                      "5y": 60 * 24 * 30 * 12 * 5}
        # calculate alpha
        # https://corporatefinanceinstitute.com/resources/knowledge/finance/alpha/
        # calculate beta
        # https://www.investopedia.com/ask/answers/070615/what-formula-calculating-beta.asp
        # calculate covariance
        # https://www.indeed.com/career-advice/career-development/how-to-calculate-covariance
        cov_matrix_df = data_period_df[[marketCol, "NetLiquidation"]]
        cov_matrix_df = cov_matrix_df[~(cov_matrix_df == 0).any(axis=1)]
        beta = self.find_beta(cov_matrix_df)

        # calculate marketreturn and portfolio return
        startNL = data_period_df["NetLiquidation"].iloc[0]
        endNL = data_period_df["NetLiquidation"].iloc[-1]
        portfolio_return = (endNL - startNL) / startNL

        # NOT GETTING 3188 Alpha, engine supposed to be dynamic.  Add a "marketCol" in input for user to input market comparison
        startR = data_period_df[marketCol].iloc[1]
        endR = data_period_df[marketCol].iloc[-1]
        marketReturn = (endR - startR) / startR

        # calculate alpha
        alpha = portfolio_return - EQV_RISK_FREE_RATE ** multiplier[lookback_period] - \
                beta * (marketReturn - EQV_RISK_FREE_RATE ** multiplier[lookback_period])

        # good , write get_alpha_data function to output all the alpha
        return alpha

    def get_alpha_by_range(self, range, file_name, marketCol):
        alpha_range_df = self.data_engine.get_data_by_range(range, file_name)
        no_of_days = (pd.to_datetime(range[1], format="%Y-%m-%d") - pd.to_datetime(range[0],
                                                                                   format="%Y-%m-%d")).days + 1
        # calculate beta
        # https://www.investopedia.com/ask/answers/070615/what-formula-calculating-beta.asp
        cov_matrix_df = alpha_range_df[[marketCol, "NetLiquidation"]]
        beta = self.find_beta(cov_matrix_df)

        # calculate marketReturn and portfolio return
        startNL = alpha_range_df["NetLiquidation"].dropna().iloc[0]
        endNL = alpha_range_df["NetLiquidation"].dropna().iloc[-1]
        portfolio_return = (endNL - startNL) / startNL

        startR = alpha_range_df[marketCol].dropna().iloc[0]
        endR = alpha_range_df[marketCol].dropna().iloc[-1]
        marketReturn = (endR - startR) / startR

        # calculate alpha
        alpha = portfolio_return - EQV_RISK_FREE_RATE ** (no_of_days * 60 * 24) - \
                beta * (marketReturn - EQV_RISK_FREE_RATE ** (no_of_days * 60 * 24))

        return alpha

    def get_alpha_data(self, file_name, marketCol):

        alpha_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"

        alpha_dict["ytd"] = self.get_alpha_ytd(file_name, marketCol)
        alpha_dict["1y"] = self.get_alpha_by_period(day_string, "1y", file_name, marketCol)
        alpha_dict["3y"] = self.get_alpha_by_period(day_string, "3y", file_name, marketCol)
        alpha_dict["5y"] = self.get_alpha_by_period(day_string, "5y", file_name, marketCol)
        alpha_dict["inception"] = self.get_alpha_inception(file_name, marketCol)

        return alpha_dict

    def get_alpha_ytd(self, file_name, marketCol):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]

        return self.get_alpha_by_range(range, file_name, marketCol)

    def get_alpha_inception(self, file_name, marketCol):

        inception_df = self.data_engine.get_full_df(file_name)
        cov_matrix_df = inception_df[[marketCol, "NetLiquidation"]]
        cov_matrix_df = cov_matrix_df[~(cov_matrix_df == 0).any(axis=1)]
        beta = self.find_beta(cov_matrix_df)
        # print(f"beta: {beta}")

        startdt = dt.datetime.fromtimestamp(inception_df['timestamp'].min())
        enddt = dt.datetime.fromtimestamp(inception_df['timestamp'].max())
        no_of_days = (enddt - startdt).days + 1

        # the first data of ML is 0 for some reason so here use the next NL data
        startNL = inception_df.loc[inception_df['timestamp'] == inception_df['timestamp'].nsmallest(2).iloc[-1]][
            'NetLiquidation'].values[0]
        endNL = inception_df.loc[inception_df['timestamp'] == inception_df['timestamp'].max()]['NetLiquidation'].values[
            0]
        portfolio_return = (endNL - startNL) / startNL

        # iloc[0] is 0 so i use iloc[1] as starting point
        startR = inception_df[marketCol].iloc[1]
        endR = inception_df[marketCol].iloc[-1]
        marketReturn = (endR - startR) / startR

        # calculate alpha
        alpha = portfolio_return - EQV_RISK_FREE_RATE ** no_of_days - \
                beta * (marketReturn - EQV_RISK_FREE_RATE ** no_of_days)

        return alpha

    def find_beta(self, cov_matrix_df):
        cov_matrix_df = cov_matrix_df.pct_change().dropna()
        x = np.array(cov_matrix_df.iloc[:, 0])
        y = np.array(cov_matrix_df.iloc[:, 1])

        # uncomment the following code to see the graph for finding beta (slope of linear regression)
        # plt.scatter(x, y)
        # plt.show()

        slope, intercept, r, p, std_err = stats.linregress(x, y)

        # def myfunc(x):
        #   return slope * x + intercept

        # mymodel = list(map(myfunc, x))

        # plt.scatter(x, y)
        # plt.plot(x, mymodel)
        # plt.show()

        return slope

    def get_rolling_return_data(self, file_name, range):

        rolling_return_dict = {}

        rolling_return_dict['1y'] = self.get_rolling_return_by_range(range, file_name, '1y')
        rolling_return_dict['2y'] = self.get_rolling_return_by_range(range, file_name, '2y')
        rolling_return_dict['3y'] = self.get_rolling_return_by_range(range, file_name, '3y')
        rolling_return_dict['5y'] = self.get_rolling_return_by_range(range, file_name, '5y')
        rolling_return_dict['7y'] = self.get_rolling_return_by_range(range, file_name, '7y')
        rolling_return_dict['10y'] = self.get_rolling_return_by_range(range, file_name, '10y')
        rolling_return_dict['15y'] = self.get_rolling_return_by_range(range, file_name, '15y')
        rolling_return_dict['20y'] = self.get_rolling_return_by_range(range, file_name, '20y')

        return rolling_return_dict

    def get_rolling_return_by_range(self, range, file_name, rolling_period):

        # return these parameters as a dictionary
        max_annual_rolling_return = float('-inf')
        min_annual_rolling_return = float('inf')
        dateinfo_index_max = float('NaN')
        dateinfo_index_min = float('NaN')
        positive = 0
        negative = 0
        avg_return = np.array([])

        if rolling_period in ['1y', '2y', '3y', '5y', '7y', '10y', '15y', '20y']:
            rolling_range_df = self.data_engine.get_data_by_range(range, file_name)

        rolling_period_dict = {'1y': 1, '2y': 2, '3y': 3, '5y': 5, '7y': 7, '10y': 10, '15y': 15, '20y': 20}
        start_dt = pd.to_datetime(range[0], format="%Y-%m-%d")
        end_dt = pd.to_datetime(range[1], format="%Y-%m-%d")

        rolling_start_dt = start_dt
        rolling_end_dt = rolling_start_dt + relativedelta(years=rolling_period_dict[rolling_period])

        # when last date of rolling is still in range then loop
        while (rolling_end_dt <= end_dt):

            start_ts = dt.datetime.timestamp(rolling_start_dt)
            end_ts = dt.datetime.timestamp(rolling_end_dt)

            start_info_df = rolling_range_df.iloc[rolling_range_df[rolling_range_df['timestamp'] >= start_ts].index[0]]
            end_info_df = rolling_range_df.iloc[rolling_range_df[rolling_range_df['timestamp'] <= end_ts].index[-1]]
            temprrs = start_info_df['NetLiquidation']
            temprre = end_info_df['NetLiquidation']

            annual_rolling_return_temp = (temprre / temprrs) ** (1 / rolling_period_dict[rolling_period]) - 1
            avg_return = np.append(avg_return, annual_rolling_return_temp)

            if (annual_rolling_return_temp > 0):
                positive += 1
            elif (annual_rolling_return_temp < 0):
                negative += 1

            if (max_annual_rolling_return < annual_rolling_return_temp):
                max_annual_rolling_return = annual_rolling_return_temp
                dateinfo_index_max = f"{start_info_df['date']} to {end_info_df['date']}"

            if (min_annual_rolling_return > annual_rolling_return_temp):
                min_annual_rolling_return = annual_rolling_return_temp
                dateinfo_index_min = f"{start_info_df['date']} to {end_info_df['date']}"

            rolling_start_dt += dt.timedelta(days=1)
            rolling_end_dt = rolling_start_dt + relativedelta(years=rolling_period_dict[rolling_period])

        if (avg_return.size != 0):
            mean = np.mean(avg_return)
        else:
            mean = float('NaN')
        if ((positive + negative) > 0):
            neg_periods = negative / (positive + negative)
        else:
            neg_periods = float('NaN')

        return {"max_annual_rolling_return": max_annual_rolling_return,
                "dateinfo_index_max": dateinfo_index_max,
                "min_annual_rolling_return": min_annual_rolling_return,
                "dateinfo_index_min": dateinfo_index_min,
                "average_annual_return": mean,
                "negative_periods": neg_periods}

    def get_volatility_by_period(self, date, lookback_period, file_name, marketCol):
        # should be using by period, like get_alpha, ask Mark how to do it
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
        delete_duplicate_df = data_period_df.drop_duplicates(subset=["date"], keep=False)
        no_of_days = delete_duplicate_df["date"].count()
        # calculate daily logarithmic return data_period_df['timestamp'].max().index.values
        pd.options.mode.chained_assignment = None
        data_period_df['returns'] = np.log(data_period_df[marketCol] / data_period_df[marketCol].shift(-1))
        # calculate daily standard deviation of returns
        daily_std = data_period_df['returns'].std()

        return daily_std * no_of_days ** 0.5

    def get_volatility_by_range(self, range, file_name, marketCol):

        range_df = self.data_engine.get_data_by_range(range, file_name)
        no_of_days = (pd.to_datetime(range[1], format="%Y-%m-%d") - pd.to_datetime(range[0],
                                                                                   format="%Y-%m-%d")).days + 1
        pd.options.mode.chained_assignment = None
        range_df['returns'] = np.log(range_df[marketCol] / range_df[marketCol].shift(-1))
        daily_std = range_df['returns'].std()

        return daily_std * no_of_days ** 0.5

    def get_volatility_inception(self, file_name, marketCol):
        inception_df = self.data_engine.get_full_df(file_name)
        if isinstance(self.data_engine, sim_data_io_engine.online_engine):
            date_column = inception_df['timestamp'].dt.date
            no_of_days = (date_column.max() - date_column.min()).days + 1
        elif isinstance(self.data_engine, sim_data_io_engine.offline_engine):
            date_column = inception_df['timestamp']
            no_of_days = (date_column.max() - date_column.min()) / (24 * 60 * 60) + 1
        inception_df['returns'] = np.log(inception_df[marketCol] / inception_df[marketCol].shift(-1))
        daily_std = inception_df['returns'].std()

        return daily_std * no_of_days ** 0.5

    def get_volatility_ytd(self, file_name, marketCol):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]

        return self.get_volatility_by_range(range, file_name, marketCol)

    def get_volatility_data(self, file_name, marketCol):
        volatility_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        volatility_dict["ytd"] = self.get_volatility_ytd(file_name, marketCol)
        volatility_dict["1y"] = self.get_volatility_by_period(day_string, "1y", file_name, marketCol)
        volatility_dict["3y"] = self.get_volatility_by_period(day_string, "3y", file_name, marketCol)
        volatility_dict["5y"] = self.get_volatility_by_period(day_string, "5y", file_name, marketCol)
        volatility_dict["inception"] = self.get_volatility_inception(file_name, marketCol)

        return volatility_dict

    def find_beta(self, cov_matrix_df):
        cov_matrix_df = cov_matrix_df.pct_change().dropna()
        x = np.array(cov_matrix_df.iloc[:, 0])
        y = np.array(cov_matrix_df.iloc[:, 1])
        slope, intercept, r, p, std_err = stats.linregress(x, y)

        return slope

    def get_win_rate_by_period(self, date, lookback_period, file_name):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
        profit = data_period_df.NetLiquidation.diff()
        no_of_winning_trade = profit[profit > 0].count()

        return no_of_winning_trade / data_period_df[
            'NetLiquidation'].count()  # number of win trades divided by total trades

    def get_win_rate_inception(self, file_name):
        inception_df = self.data_engine.get_full_df(file_name)
        profit = inception_df.NetLiquidation.diff()
        no_of_winning_trade = profit[profit > 0].count()

        return no_of_winning_trade / inception_df['NetLiquidation'].count()

    def get_win_rate_data(self, file_name):
        win_rate_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        win_rate_dict["1y"] = self.get_win_rate_by_period(day_string, "1y", file_name)
        win_rate_dict["3y"] = self.get_win_rate_by_period(day_string, "3y", file_name)
        win_rate_dict["5y"] = self.get_win_rate_by_period(day_string, "5y", file_name)
        win_rate_dict["inception"] = self.get_win_rate_inception(file_name)

        return win_rate_dict

    def get_loss_rate_by_period(self, date, lookback_period, file_name):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
        profit = data_period_df.NetLiquidation.diff()
        no_of_loss_trade = profit[profit < 0].count()

        return no_of_loss_trade / data_period_df[
            'NetLiquidation'].count()  # number of win trades divided by total trades

    def get_loss_rate_inception(self, file_name):
        inception_df = self.data_engine.get_full_df(file_name)
        profit = inception_df.NetLiquidation.diff()
        no_of_loss_trade = profit[profit < 0].count()

        return no_of_loss_trade / inception_df['NetLiquidation'].count()

    def get_loss_rate_data(self, file_name):
        loss_rate_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        loss_rate_dict["1y"] = self.get_loss_rate_by_period(day_string, "1y", file_name)
        loss_rate_dict["3y"] = self.get_loss_rate_by_period(day_string, "3y", file_name)
        loss_rate_dict["5y"] = self.get_loss_rate_by_period(day_string, "5y", file_name)
        loss_rate_dict["inception"] = self.get_loss_rate_inception(file_name)

        return loss_rate_dict

    def get_total_trade(self, file_name, actionCol):
        full_df = self.data_engine.get_full_df(file_name)
        full_df['total_trade'] = full_df[actionCol]

        return full_df.total_trade.value_counts().Buy + full_df.total_trade.value_counts().sell

    def get_compounding_annual_return(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        full_df_arr = np.array(full_df['NetLiquidation'])

        return (full_df_arr[-1] / full_df_arr[0]) ** 0.25 - 1

    def get_treynor_ratio_by_period(self, date, lookback_period, file_name, marketCol):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
        startNL = data_period_df["NetLiquidation"].iloc[0]
        endNL = data_period_df["NetLiquidation"].iloc[-1]
        portfolio_return = (endNL - startNL) / startNL
        multiplier = {"1d": 60 * 24, "1m": 60 * 24 * 30, "6m": 60 * 24 * 30 * 6, "1y": 60 * 24 * 30 * 12,
                      "3y": 60 * 24 * 30 * 12 * 3,
                      "5y": 60 * 24 * 30 * 12 * 5}
        cov_matrix_df = data_period_df[[marketCol, "NetLiquidation"]]
        cov_matrix_df = cov_matrix_df[~(cov_matrix_df == 0).any(axis=1)]
        beta = self.find_beta(cov_matrix_df)

        return (portfolio_return - EQV_RISK_FREE_RATE ** multiplier[lookback_period]) / beta

    def get_treynor_ratio_by_range(self, range, file_name, marketCol):
        range_df = self.data_engine.get_data_by_range(range, file_name)
        no_of_days = (pd.to_datetime(range[1], format="%Y-%m-%d") - pd.to_datetime(range[0],
                                                                                   format="%Y-%m-%d")).days + 1
        startNL = range_df["NetLiquidation"].iloc[0]
        endNL = range_df["NetLiquidation"].iloc[-1]
        portfolio_return = (endNL - startNL) / startNL
        cov_matrix_df = range_df[[marketCol, "NetLiquidation"]]
        beta = self.find_beta(cov_matrix_df)

        return (portfolio_return - EQV_RISK_FREE_RATE ** (no_of_days * 60 * 24)) / beta

    def get_treynor_ratio_inception(self, file_name, marketCol):
        inception_df = self.data_engine.get_full_df(file_name)
        cov_matrix_df = inception_df[[marketCol, "NetLiquidation"]]
        cov_matrix_df = cov_matrix_df[~(cov_matrix_df == 0).any(axis=1)]
        beta = self.find_beta(cov_matrix_df)
        startdt = dt.datetime.fromtimestamp(inception_df['timestamp'].min())
        enddt = dt.datetime.fromtimestamp(inception_df['timestamp'].max())
        no_of_days = (enddt - startdt).days + 1
        startNL = inception_df["NetLiquidation"].iloc[0]
        endNL = inception_df["NetLiquidation"].iloc[-1]
        portfolio_return = (endNL - startNL) / startNL

        return (portfolio_return - EQV_RISK_FREE_RATE ** no_of_days) / beta

    def get_treynor_ratio_ytd(self, file_name, marketCol):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]

        return self.get_treynor_ratio_by_range(range, file_name, marketCol)

    def get_treynor_ratio_data(self, file_name, marketCol):
        treynor_ratio_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        treynor_ratio_dict["ytd"] = self.get_treynor_ratio_ytd(file_name, marketCol)
        treynor_ratio_dict["1y"] = self.get_treynor_ratio_by_period(day_string, "1y", file_name, marketCol)
        treynor_ratio_dict["3y"] = self.get_treynor_ratio_by_period(day_string, "3y", file_name, marketCol)
        treynor_ratio_dict["5y"] = self.get_treynor_ratio_by_period(day_string, "5y", file_name, marketCol)
        treynor_ratio_dict["inception"] = self.get_treynor_ratio_inception(file_name, marketCol)

        return treynor_ratio_dict

    def get_information_ratio_by_period(self, date, lookback_period, file_name, marketCol):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
        average_portfolio_return = data_period_df["NetLiquidation"].pct_change().mean()
        average_benchmark_return = data_period_df[marketCol].pct_change().mean()
        track_error = data_period_df["NetLiquidation"].pct_change().std() - data_period_df[marketCol].pct_change().std()

        return (average_portfolio_return - average_benchmark_return) / track_error

    def get_information_ratio_by_range(self, range, file_name, marketCol):
        range_df = self.data_engine.get_data_by_range(range, file_name)
        average_portfolio_return = range_df["NetLiquidation"].pct_change().mean()
        average_benchmark_return = range_df[marketCol].pct_change().mean()
        track_error = range_df["NetLiquidation"].pct_change().std() - range_df[marketCol].pct_change().std()

        return (average_portfolio_return - average_benchmark_return) / track_error

    def get_information_ratio_inception(self, file_name, marketCol):
        inception_df = self.data_engine.get_full_df(file_name)
        average_portfolio_return = inception_df["NetLiquidation"].pct_change().mean()
        average_benchmark_return = inception_df[marketCol].pct_change().mean()
        track_error = inception_df["NetLiquidation"].pct_change().std() - inception_df[marketCol].pct_change().std()

        return (average_portfolio_return - average_benchmark_return) / track_error

    def get_information_ratio_ytd(self, file_name, marketCol):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]

        return self.get_information_ratio_by_range(range, file_name, marketCol)

    def get_information_ratio_data(self, file_name, marketCol):
        information_ratio_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        information_ratio_dict["ytd"] = self.get_information_ratio_ytd(file_name, marketCol)
        information_ratio_dict["1y"] = self.get_information_ratio_by_period(day_string, "1y", file_name, marketCol)
        information_ratio_dict["3y"] = self.get_information_ratio_by_period(day_string, "3y", file_name, marketCol)
        information_ratio_dict["5y"] = self.get_information_ratio_by_period(day_string, "5y", file_name, marketCol)
        information_ratio_dict["inception"] = self.get_information_ratio_inception(file_name, marketCol)

        return information_ratio_dict

    def get_drawdown_data(self, file_name, range):

        # drawdown_dict = {}
        #
        # drawdown_dict['drawdown_abstract'] = self.get_drawdown_by_range(range ,file_name)
        # drawdown_dict['drawdown_raw_data'] = self.get_drawdown_raw_data_by_range(file_name, range)

        drawdown_df = self.get_drawdown_by_range(range ,file_name)
        drawdown_raw_df = self.get_drawdown_raw_data_by_range(file_name, range)
        return drawdown_df, drawdown_raw_df

    def get_drawdown_by_range(self, range, file_name):
        drawdown_df = pd.DataFrame(
            columns=["Drawdown", "Drawdown period", "Drawdown days", "Recovery date", "Recovery days"])
        range_df = self.data_engine.get_data_by_range(range, file_name)

        start_dt = pd.to_datetime(range[0], format="%Y-%m-%d")
        end_dt = pd.to_datetime(range[1], format="%Y-%m-%d")
        current_dt = start_dt

        start_ts = dt.datetime.timestamp(start_dt)
        end_ts = dt.datetime.timestamp(end_dt)

        g_max = -np.inf
        g_min = np.inf

        info_df = range_df.loc[(range_df['timestamp'] >= start_ts) & (range_df['timestamp'] <= end_ts)]

        for index, row in info_df.iterrows():
            if ((row['NetLiquidation']) >= g_max):
                if (not np.isinf(g_min)):
                    recovery_date_info = row['date']
                    max_drawdown = (g_min - g_max) / g_max
                    drawdown_period = [max_date_info, min_date_info]
                    drawdown_days = (pd.to_datetime(min_date_info, format="%Y-%m-%d") - pd.to_datetime(max_date_info,
                                                                                                       format="%Y-%m-%d")).days
                    recovery_days = (
                                pd.to_datetime(recovery_date_info, format="%Y-%m-%d") - pd.to_datetime(min_date_info,
                                                                                                       format="%Y-%m-%d")).days
                    list = [max_drawdown, drawdown_period, drawdown_days, recovery_date_info, recovery_days]
                    drawdown_df.loc[len(drawdown_df.index)] = list
                    g_min = np.inf
                g_max = row['NetLiquidation']
                max_date_info = row['date']

            else:
                if (row['NetLiquidation'] < g_min):
                    g_min = row['NetLiquidation']
                    min_date_info = row['date']

        if (not np.isinf(g_min)):
            recovery_date_info = np.nan
            max_drawdown = (g_min - g_max) / g_max
            drawdown_period = [max_date_info, min_date_info]
            drawdown_days = (pd.to_datetime(min_date_info, format="%Y-%m-%d") - pd.to_datetime(max_date_info, \
                                                                                               format="%Y-%m-%d")).days
            recovery_days = np.nan
            list = [max_drawdown, drawdown_period, drawdown_days, recovery_date_info, recovery_days]
            drawdown_df.loc[len(drawdown_df.index)] = list

        p = (drawdown_df.sort_values(by=['Drawdown'])).reset_index(drop= True)
        return p

    def get_drawdown_raw_data_by_range(self,file_name, range):
        # drawdown_df = pd.DataFrame(columns = ["Drawdown","Drawdown period","Drawdown days","Recovery date", "Recovery days"])
        range_df = self.data_engine.get_data_by_range(range, file_name)
        output_df = pd.DataFrame(columns=['timestamp', 'drawdown'])

        start_dt = pd.to_datetime(range[0], format="%Y-%m-%d")
        end_dt = pd.to_datetime(range[1], format="%Y-%m-%d")

        start_ts = dt.datetime.timestamp(start_dt)
        end_ts = dt.datetime.timestamp(end_dt)

        g_max = -np.inf

        info_df = range_df.loc[(range_df['timestamp'] >= start_ts) & (range_df['timestamp'] <= end_ts)]
        info_df["drawdown"] = np.nan

        for index, row in info_df.iterrows():
            if (row['NetLiquidation']) >= g_max:
                g_max = row['NetLiquidation']

            if (g_max - row['NetLiquidation']) <= 0:
                output_df.at[index, "drawdown"] = 0
                output_df.at[index, "timestamp"] = row['timestamp']
            else:
                output_df.at[index, "drawdown"] = (row['NetLiquidation'] - g_max) / g_max
                output_df.at[index, "timestamp"] = row['timestamp']

        return output_df

    def get_average_win_day_by_period(self, date, lookback_period, file_name):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
        # print(data_period_df)
        results = self.check_win_or_lose_day(data_period_df)
        # average_win_day = results[0]
        # average_lose_day = results[1]
        return results[0]

    def get_average_win_day_by_range(self, rangee, file_name):
        range_df = self.data_engine.get_data_by_range(rangee, file_name)
        # print(range_df)
        results = self.check_win_or_lose_day(range_df)
        # average_win_day = results[0]
        # average_lose_day = results[1]
        return results[0]

    def get_average_win_day_ytd(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]

        return self.get_average_win_day_by_range(range, file_name)

    def get_average_win_day_inception(self, file_name):
        inception_df = self.data_engine.get_full_df(file_name)
        # print(inception_df)
        results = self.check_win_or_lose_day(inception_df)
        # average_win_day = results[0]
        # average_lose_day = results[1]
        return results[0]

    def get_average_win_day_data(self, file_name):
        average_win_day_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        average_win_day_dict["ytd"] = self.get_average_win_day_ytd(file_name)
        average_win_day_dict["1y"] = self.get_average_win_day_by_period(day_string, "1y", file_name)
        average_win_day_dict["3y"] = self.get_average_win_day_by_period(day_string, "3y", file_name)
        average_win_day_dict["5y"] = self.get_average_win_day_by_period(day_string, "5y", file_name)
        average_win_day_dict["inception"] = self.get_average_win_day_inception(file_name)

        return average_win_day_dict

    def get_profit_loss_ratio_by_period(self, date, lookback_period, file_name):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
        # print(data_period_df)
        results = self.check_win_or_lose_day(data_period_df)
        # average_win_day = results[0]
        # average_lose_day = results[1]
        # print(results)
        if results[0] == 0 or results[1] == 0:
            profit_loss_ratio = 0
        else:
            profit_loss_ratio = results[0] / results[1]

        return profit_loss_ratio

    def get_profit_loss_ratio_by_range(self, rangee, file_name):
        range_df = self.data_engine.get_data_by_range(rangee, file_name)
        # print(range_df)
        results = self.check_win_or_lose_day(range_df)
        # print(results)
        # average_win_day = results[0]
        # average_lose_day = results[1]
        if results[0] == 0 or results[1] == 0:
            profit_loss_ratio = 0
        else:
            profit_loss_ratio = results[0] / results[1]

        return profit_loss_ratio

    def get_profit_loss_ratio_ytd(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]

        return self.get_profit_loss_ratio_by_range(range, file_name)

    def get_profit_loss_ratio_inception(self, file_name):
        inception_df = self.data_engine.get_full_df(file_name)
        # print(inception_df)
        results = self.check_win_or_lose_day(inception_df)
        # average_win_day = results[0]
        # average_lose_day = results[1]
        if results[0] == 0 or results[1] == 0:
            profit_loss_ratio = 0
        else:
            profit_loss_ratio = results[0] / results[1]

        return profit_loss_ratio

    def get_profit_loss_ratio_data(self, file_name):
        profit_loss_ratio_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        profit_loss_ratio_dict["ytd"] = self.get_profit_loss_ratio_ytd(file_name)
        profit_loss_ratio_dict["1y"] = self.get_profit_loss_ratio_by_period(day_string, "1y", file_name)
        profit_loss_ratio_dict["3y"] = self.get_profit_loss_ratio_by_period(day_string, "3y", file_name)
        profit_loss_ratio_dict["5y"] = self.get_profit_loss_ratio_by_period(day_string, "5y", file_name)
        profit_loss_ratio_dict["inception"] = self.get_profit_loss_ratio_inception(file_name)

        return profit_loss_ratio_dict

    def check_win_or_lose_day(self, df, sum_of_percentage_increased=0, number_of_win_days=0,
                              sum_of_percentage_decreased=0, number_of_lose_days=0, pos=0):
        for x in range(len(df['date'])):
            check = False
            # if x == len(df['date']) - 1:
            #     percentage_change_in_net_liquidation = (df['NetLiquidation'][x] - df['NetLiquidation'][pos]) / df['NetLiquidation'][pos]
            #     # print(range_df['NetLiquidation'][x], 1)
            #     # print(pos)
            #     print(percentage_change_in_net_liquidation, 1)
            #     check = True
            if df['date'][pos] != df['date'][x]:
                percentage_change_in_net_liquidation = (df['NetLiquidation'][x] - df['NetLiquidation'][pos]) / \
                                                       df['NetLiquidation'][pos]
                # print(range_df['NetLiquidation'][x], 2)
                # print(pos)
                # print(percentage_change_in_net_liquidation, 2)
                check = True
                pos = x
            # print(percentage_change_in_net_liquidation)
            if check is True:
                if percentage_change_in_net_liquidation > 0:
                    sum_of_percentage_increased += percentage_change_in_net_liquidation
                    number_of_win_days += 1
                elif percentage_change_in_net_liquidation < 0:
                    sum_of_percentage_decreased += percentage_change_in_net_liquidation
                    number_of_lose_days += 1
        if number_of_win_days == 0:
            average_win_day = 0
        else:
            average_win_day = sum_of_percentage_increased / number_of_win_days
        if number_of_lose_days == 0:
            average_lose_day = 0
        else:
            average_lose_day = abs(sum_of_percentage_decreased / number_of_lose_days)
        return [average_win_day, average_lose_day]

    def get_composite_data(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day_info = full_df[full_df['timestamp'] == full_df['timestamp'].max()]
        header = [col for col in full_df if col.startswith('marketValue')]
        gross = last_day_info.iloc[0]['GrossPositionValue']
        composite = {}
        for i in header:
            mkv = last_day_info.iloc[0][i]
            tname = (i.split("_"))[-1]
            composite.update({tname: mkv/gross})

        return composite

    def get_last_nlv(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        nlv = full_df[full_df['timestamp'] == full_df['timestamp'].max()]['NetLiquidation'].values[0]
        return nlv

    def get_last_daily_change(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        full_df['date'] = pd.to_datetime(full_df['date'])
        mask = full_df[(full_df['date'].max() - full_df['date'])/ np.timedelta64(1,'D') < 1]
        daily = (mask['NetLiquidation'].iloc[0] - mask['NetLiquidation'].iloc[-1])/mask['NetLiquidation'].iloc[0]
        return daily

    def get_last_monthly_change(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        full_df['date'] = pd.to_datetime(full_df['date'])
        mask = full_df[(full_df['date'].max() - full_df['date']) / np.timedelta64(1, 'M') < 1]
        monthly = (mask['NetLiquidation'].iloc[0] - mask['NetLiquidation'].iloc[-1]) / mask['NetLiquidation'].iloc[0]
        return monthly

    def get_sd_by_period(self, date, lookback_period, file_name):
        if lookback_period in ['1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)

        sd = data_period_df['NetLiquidation'].std() / data_period_df['NetLiquidation'].mean()
        return sd

    def get_sd_Inception(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)

        sd = full_df['NetLiquidation'].std() / full_df['NetLiquidation'].mean()
        return sd

    def get_sd_data(self, file_name):
        sd_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        sd_dict["1y"] = self.get_sd_by_period(day_string, "1y", file_name)
        sd_dict["3y"] = self.get_sd_by_period(day_string, "3y", file_name)
        sd_dict["5y"] = self.get_sd_by_period(day_string, "5y", file_name)
        sd_dict["inception"] = self.get_sd_Inception(file_name)

        return sd_dict

    def get_pos_neg_data(self, file_name):
        pos_neg_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        pos_neg_dict["1y"] = self.get_pos_neg_months_by_period(day_string, "1y", file_name)
        pos_neg_dict["3y"] = self.get_pos_neg_months_by_period(day_string, "3y", file_name)
        pos_neg_dict["5y"] = self.get_pos_neg_months_by_period(day_string, "5y", file_name)
        pos_neg_dict["inception"] = self.get_pos_neg_months_inception(file_name)

        return pos_neg_dict

    def get_pos_neg_months_inception(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        full_df['date'] = pd.to_datetime(full_df['date'])
        start_info = full_df.iloc[0]
        start_dt = full_df['date'][0]
        temp_dt = full_df['date'][0]
        temp_info = full_df.iloc[0]
        pos = 0
        neg = 0

        for i , j in full_df.iterrows():
            if start_dt.month != temp_dt.month:
                print(temp_info['date'])
                if start_info['NetLiquidation'] > temp_info['NetLiquidation']:

                    pos = pos + 1
                elif start_info['NetLiquidation'] < temp_info['NetLiquidation']:
                    neg = neg + 1
                else:
                    pass

                start_info = j
                start_dt = j['date']

            temp_dt = j['date']
            temp_info = j
        return pos, neg

    def get_pos_neg_months_by_period(self, date, lookback_period,file_name):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)

        start_info = data_period_df.iloc[0]
        start_dt = data_period_df['date'][0]
        temp_dt = data_period_df['date'][0]
        temp_info = data_period_df.iloc[0]
        pos = 0
        neg = 0

        for i, j in data_period_df.iterrows():
            if start_dt.month != temp_dt.month:
                print(temp_info['date'])
                if start_info['NetLiquidation'] > temp_info['NetLiquidation']:

                    pos = pos + 1
                elif start_info['NetLiquidation'] < temp_info['NetLiquidation']:
                    neg = neg + 1
                else:
                    pass

                start_info = j
                start_dt = j['date']

            temp_dt = j['date']
            temp_info = j
        return pos, neg


def main():
    engine = sim_data_io_engine.offline_engine('/Users/chansiuchung/Documents/IndexTrade/user_id_0/backtest/backtest_rebalance_margin_wif_max_drawdown_control_0/run_data')

    my_stat_engine = statistic_engine(engine)
    # print(isinstance(engine,sim_data_io_engine.offline_engine))
    range = ["2019-12-1", "2022-4-29"]
    print(my_stat_engine.get_pos_neg_months('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    #print(my_stat_engine.get_sd_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_sd_by_period('2020-11-30', '1y',
    #                                       '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_sd_by_period('2020-11-30', '3y',
    #                                       '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_sd_by_period('2020-11-30','5y','0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_sd_Inception('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    #print(my_stat_engine.get_last_monthly_change('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_return_range(range))
    # print(my_stat_engine.get_return_range(range,spec="0.03_rebalance_margin_0.01_maintain_margin_0.03max_drawdown__year_2011"))
    # print(my_stat_engine.get_return_range(range,spec="0.055_rebalance_margin_0.01_maintain_margin_0.01max_drawdown__purchase_exliq_5.0"))
    # print(my_stat_engine.get_return_range(range,spec="0.21_rebalance_margin_0.01_maintain_margin_0.02max_drawdown__year_2011"))
    # print(my_stat_engine.get_sharpe_by_period("2017-11-30","1m"))
    # print(my_stat_engine.get_sharpe_by_period("2017-11-30","1m",spec="0.03_rebalance_margin_0.01_maintain_margin_0.03max_drawdown__year_2011"))
    # print(my_stat_engine.get_sharpe_by_period("2017-11-30","1m",spec="0.055_rebalance_margin_0.01_maintain_margin_0.01max_drawdown__purchase_exliq_5.0"))
    # print(my_stat_engine.get_sharpe_by_period("2017-11-30","1m",spec="0.21_rebalance_margin_0.01_maintain_margin_0.02max_drawdown__year_2011"))
    # print(my_stat_engine.get_sharpe_by_range(range))
    # print(my_stat_engine.get_sharpe_by_range(range,spec="0.03_rebalance_margin_0.01_maintain_margin_0.03max_drawdown__year_2011"))
    # print(my_stat_engine.get_sharpe_by_range(range,spec="0.055_rebalance_margin_0.01_maintain_margin_0.01max_drawdown__purchase_exliq_5.0"))
    # print(my_stat_engine.get_sharpe_by_range(range,spec="0.21_rebalance_margin_0.01_maintain_margin_0.02max_drawdown__year_2011"))
    # print(my_stat_engine.get_sharpe_inception())
    # print(my_stat_engine.get_sharpe_inception(spec="0.03_rebalance_margin_0.01_maintain_margin_0.03max_drawdown__year_2011"))
    # print(my_stat_engine.get_sharpe_inception(spec="0.055_rebalance_margin_0.01_maintain_margin_0.01max_drawdown__purchase_exliq_5.0"))
    # print(my_stat_engine.get_sharpe_inception(spec="0.21_rebalance_margin_0.01_maintain_margin_0.02max_drawdown__year_2011"))
    # print(my_stat_engine.get_return_ytd(spec="0.03_rebalance_margin_0.01_maintain_margin_0.03max_drawdown__year_2011"))
    # with open(
    #         "C:\\dynamodb\\dynamodb_related\\pythonProject\\algo\\rebalance_margin_wif_max_drawdown_control\\backtest\\backtest_data\\backtest_rebalance_margin_wif_max_drawdown_control_0\\0.038_rebalance_margin_0.01_maintain_margin_0.001_max_drawdown_ratio_5.0_purchase_exliq_.csv",
    #         'r') as f:
    #     df = pd.read_csv(f)
    # print(df)
    # print(my_stat_engine.get_sortino_by_range(range, '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_alpha_by_period("2022-05-26", '5y', '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_', "3188 marketPrice"))
    # print(my_stat_engine.get_alpha_by_range(range,  '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 marketPrice"))
    # print(my_stat_engine.get_alpha_inception('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 marketPrice"))
    # print('alpha :' + str(my_stat_engine.get_alpha_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 marketPrice")))
    # # test the result in all_file_return, and add columns to
    # print('volatility :' + str(my_stat_engine.get_volatility_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 mktPrice")))
    # # print(my_stat_engine.get_rolling_return_by_range(range,'0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"5y"))
    # # my_stat_engine.get_drawdown_by_range(range, '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_')
    #print(my_stat_engine.get_drawdown_data( '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',range))
    # # print(my_stat_engine.composite('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # #print('max drawdown :' + str(my_stat_engine.get_max_drawdown_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_')))
    # print('rolling return :' + str(my_stat_engine.get_rolling_return_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',range)))
    # print('win rate :' + str(my_stat_engine.get_win_rate_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_')))
    # print('loss rate :' + str(my_stat_engine.get_loss_rate_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_')))
    # print('total trade :' + str(my_stat_engine.get_total_trade('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_','3188 action')))
    # print('compounding annual return :' + str(my_stat_engine.get_compounding_annual_return('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_')))
    # print('treynor ratio :' + str(my_stat_engine.get_treynor_ratio_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 marketPrice")))
    # print('information ratio :' + str(my_stat_engine.get_information_ratio_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 mktPrice")))
    # # print(my_stat_engine.get_profit_loss_ratio_by_period("2022-04-28", '1m','0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # # print(my_stat_engine.get_profit_loss_ratio_by_range(["2022-03-27", "2022-4-28"],'0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # # print(my_stat_engine.get_profit_loss_ratio_ytd('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # # print(my_stat_engine.get_profit_loss_ratio_inception('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print('profit loss ratio :' + str(my_stat_engine.get_profit_loss_ratio_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_')))
    # print('average win :' + str(my_stat_engine.get_average_win_day_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_')))
    # # print(my_stat_engine.get_average_win_by_period("2022-05-26", '5y', '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # # print(my_stat_engine.get_average_win_day_by_period("2022-04-28", '1m', '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # # print(my_stat_engine.get_average_win_day_by_range(["2022-03-27", "2022-4-28"], '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # # print(my_stat_engine.get_average_win_day_ytd('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # # print(my_stat_engine.get_average_win_day_inception('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    #print(my_stat_engine.get_composite_data('50_SPY_50_IVV_'))
    #print(my_stat_engine.get_return_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))

if __name__ == "__main__":
    main()
