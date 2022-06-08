import pandas as pd
import numpy as np
import os
import sys
import datetime as dt
from matplotlib import pyplot as plt
from scipy import stats
from dateutil.relativedelta import relativedelta

from engine.simulation_engine import sim_data_io_engine

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
        ending_net_liquidity = \
            inception_df.loc[inception_df['timestamp'] == inception_df['timestamp'].max()]['NetLiquidation'].values[0]
        print(f"starting_net_liquidity:{starting_net_liquidity}; ending_net_liquidity:{ending_net_liquidity}")
        return (ending_net_liquidity - starting_net_liquidity) / starting_net_liquidity

    def get_return_ytd(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]
        return self.get_return_by_range(range, file_name)

    # return a dictionary of all return info (ytd, 1y, 3y, 5y and inception)
    def get_return_data(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"

        return_dict = {}
        return_dict["ytd"] = self.get_return_ytd(file_name)
        return_dict["1y"] = self.get_return_by_period(day_string, "1y", file_name)
        return_dict["3y"] = self.get_return_by_period(day_string, "3y", file_name)
        return_dict["5y"] = self.get_return_by_period(day_string, "5y", file_name)
        return_dict["inception"] = self.get_return_inception(file_name)

        return return_dict

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
        alpha = portfolio_return - EQV_RISK_FREE_RATE ** multiplier[lookback_period]  - \
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
        alpha = portfolio_return - EQV_RISK_FREE_RATE ** (no_of_days*60*24)- \
                beta * (marketReturn - EQV_RISK_FREE_RATE ** (no_of_days*60*24))

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
        cov_matrix_df = cov_matrix_df[~(cov_matrix_df == 0).any(axis = 1)]
        beta = self.find_beta(cov_matrix_df)
        print(f"beta: {beta}")

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
        max_rolling_return = float('-inf')
        min_rolling_return = float('inf')
        dateinfo_index_max = float('NaN')
        dateinfo_index_min = float('NaN')
        avg_return = np.array([])

        if rolling_period in ['1y', '2y', '3y', '5y', '7y', '10y', '15y', '20y']:
            rolling_range_df = self.data_engine.get_data_by_range(range, file_name)

        rolling_period_dict = {'1y': 1, '2y':2, '3y':3, '5y':5, '7y':7, '10y':10, '15y':15, '20y':20}
        start_dt = pd.to_datetime(range[0],format="%Y-%m-%d")
        end_dt = pd.to_datetime(range[1],format="%Y-%m-%d")

        rolling_start_dt = start_dt
        rolling_end_dt = rolling_start_dt + relativedelta(years = rolling_period_dict[rolling_period])

        #when last date of rolling is still in range then loop
        while(rolling_end_dt <= end_dt):

            start_ts = dt.datetime.timestamp(rolling_start_dt)
            end_ts = dt.datetime.timestamp(rolling_end_dt)

            start_info_df = rolling_range_df.iloc[rolling_range_df[rolling_range_df['timestamp'] >= start_ts].index[0]]
            end_info_df = rolling_range_df.iloc[rolling_range_df[rolling_range_df['timestamp'] <= end_ts].index[-1]]
            temprrs = start_info_df['NetLiquidation']
            temprre = end_info_df['NetLiquidation']

            rolling_return_temp = (temprre - temprrs) / temprrs
            avg_return = np.append(avg_return, rolling_return_temp)

            if(max_rolling_return < rolling_return_temp):
                max_rolling_return = rolling_return_temp
                dateinfo_index_max = f"{start_info_df['date']} to {end_info_df['date']}"

            if(min_rolling_return > rolling_return_temp):
                min_rolling_return = rolling_return_temp
                dateinfo_index_min = f"{start_info_df['date']} to {end_info_df['date']}"


            rolling_start_dt += dt.timedelta(days=1)
            rolling_end_dt = rolling_start_dt + relativedelta(years = rolling_period_dict[rolling_period])

        if(avg_return.size != 0):
            mean = np.mean(avg_return)
        else:
            mean = float('NaN')

        return {"max_rolling_return": max_rolling_return,
                "dateinfo_index_max": dateinfo_index_max,
                "min_rolling_return": min_rolling_return,
                "dateinfo_index_min": dateinfo_index_min,
                "average_return": mean}

    def get_volatility_by_period(self,date,lookback_period,file_name,marketCol):
        # should be using by period, like get_alpha, ask Mark how to do it
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)

        # calculate daily logarithmic return
        pd.options.mode.chained_assignment = None
        data_period_df['returns'] = np.log(data_period_df[marketCol] / data_period_df[marketCol].shift(-1))
        # calculate daily standard deviation of returns
        daily_std = data_period_df['returns'].std()

        return daily_std * 252 ** 0.5

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


def main():
    engine = sim_data_io_engine.offline_engine('/Users/chansiuchung/Documents/IndexTrade/user_id_0/backtest/backtest_rebalance_margin_wif_max_drawdown_control_0/run_data')

    my_stat_engine = statistic_engine(engine)
    # print(isinstance(engine,sim_data_io_engine.offline_engine))
    range = ["2012-12-1", "2021-12-1"]
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
    #print(my_stat_engine.get_sortino_by_range(range, '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    #print(my_stat_engine.get_alpha_by_period("2022-05-26", '5y', '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_', "3188 marketPrice"))
    #print(my_stat_engine.get_alpha_by_range(range,  '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 marketPrice"))
    #print(my_stat_engine.get_alpha_inception('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 marketPrice"))
    #print(my_stat_engine.get_alpha_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 marketPrice"))
    #test the result in all_file_return, and add columns to
    #print(my_stat_engine.get_volatility_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    print(my_stat_engine.get_rolling_return_by_range(range,'0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"10y"))
if __name__ == "__main__":
    main()
