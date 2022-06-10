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


class statistic_engine_2:
    # private data members # modify later
    data_engine = None

    def __init__(self, data_engine):
        self.data_engine = data_engine

    def find_beta(self, cov_matrix_df):
        cov_matrix_df = cov_matrix_df.pct_change().dropna()
        x = np.array(cov_matrix_df.iloc[:, 0])
        y = np.array(cov_matrix_df.iloc[:, 1])
        slope, intercept, r, p, std_err = stats.linregress(x, y)

        return slope

    def get_win_rate_by_period(self, date, lookback_period, file_name):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
        profit = data_period_df.NetLiquidation.diff() * 100
        no_of_winning_trade = profit[profit > 0].count()

        return no_of_winning_trade / data_period_df['NetLiquidation'].count()  # number of win trades divided by total trades

    def get_win_rate_by_range(self, range, file_name):
        range_df = self.data_engine.get_data_by_range(range, file_name)
        profit = range_df.NetLiquidation.diff() * 100
        no_of_winning_trade = profit[profit > 0].count()

        return no_of_winning_trade / range_df['NetLiquidation'].count()

    def get_win_rate_inception(self, file_name):
        inception_df = self.data_engine.get_full_df(file_name)
        profit = inception_df.NetLiquidation.diff() * 100
        no_of_winning_trade = profit[profit > 0].count()

        return no_of_winning_trade / inception_df['NetLiquidation'].count()

    def get_win_rate_ytd(self, file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01", f"{year}-{month}-{day}"]

        return self.get_win_rate_by_range(range, file_name)

    def get_win_rate_data(self, file_name):
        win_rate_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        win_rate_dict["ytd"] = self.get_win_rate_ytd(file_name)
        win_rate_dict["1y"] = self.get_win_rate_by_period(day_string, "1y", file_name)
        win_rate_dict["3y"] = self.get_win_rate_by_period(day_string, "3y", file_name)
        win_rate_dict["5y"] = self.get_win_rate_by_period(day_string, "5y", file_name)
        win_rate_dict["inception"] = self.get_win_rate_inception(file_name)

        return win_rate_dict

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


def main():
    engine = sim_data_io_engine.offline_engine(
        '/Users/percychui/Documents/Rainy Drop/user_id_0/backtest/backtest_rebalance_margin_wif_max_drawdown_control_0/run_data')

    my_stat_engine = statistic_engine_2(engine)
    # print(isinstance(engine,sim_data_io_engine.offline_engine))
    range = ["2015-12-1", "2021-12-1"]
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
    # print(my_stat_engine.get_alpha_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 marketPrice"))
    # test the result in all_file_return, and add columns to
    # print(my_stat_engine.get_volatility_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_win_rate_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_total_trade('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_','3188 action'))
    # print(my_stat_engine.get_compounding_annual_return('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_treynor_ratio_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"3188 marketPrice"))


if __name__ == "__main__":
    main()
