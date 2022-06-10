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


class statistic_engine_3:
    # private data members # modify later
    data_engine = None

    def __init__(self, data_engine):
        self.data_engine = data_engine

    # def get_average_win_by_period(self, date, lookback_period, file_name):
    #     if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
    #         data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
    #     pd.options.mode.chained_assignment = None #?
    #     realized_pnl_list = data_period_df['3188 realizedPNL'].tolist()
    #     number_of_win_trades = 0
    #     sum_of_win_trades = 0
    #     for x in range(len(realized_pnl_list)-1):
    #         if realized_pnl_list[x+1] > realized_pnl_list[x]:
    #             number_of_win_trades += 1
    #             sum_of_win_trades += realized_pnl_list[x+1] - realized_pnl_list[x]
    #     average_win = sum_of_win_trades / number_of_win_trades
    #     return average_win

    def get_average_win_day_by_period(self, date, lookback_period, file_name):
        if lookback_period in ['1d', '1m', '6m', '1y', '3y', '5y']:
            data_period_df = self.data_engine.get_data_by_period(date, lookback_period, file_name)
        #print(data_period_df)
        results = self.check_win_or_lose_day(data_period_df)
        #if number_of_win_days != 0:
        #else:
            #average_win_day = 0
        # average_win_day = results[0]
        # average_lose_day = results[1]
        return results[0]

    def get_average_win_day_by_range(self, rangee, file_name):
        range_df = self.data_engine.get_data_by_range(rangee, file_name)
        #print(range_df)
        results = self.check_win_or_lose_day(range_df)
        # if number_of_win_days != 0:
        # else:
            # average_win_day = 0
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
        # if number_of_win_days != 0:
        # else:
        # average_win_day = 0
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
        #print(data_period_df)
        results = self.check_win_or_lose_day(data_period_df)
        # average_win_day = results[0]
        # average_lose_day = results[1]
        if results[0] == 0 or results[1] == 0:
            profit_loss_ratio = 0
        else:
            profit_loss_ratio = results[0] / results[1]

        return profit_loss_ratio

    def get_profit_loss_ratio_by_range(self, rangee, file_name):
        range_df = self.data_engine.get_data_by_range(rangee, file_name)
        #print(range_df)
        results = self.check_win_or_lose_day(range_df)
        print(results)
        #average_win_day = results[0]
        #average_lose_day = results[1]
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

    def check_win_or_lose_day(self, df, pos=0, check=False, sum_of_percentage_increased=0, number_of_win_days=0, sum_of_percentage_decreased=0, number_of_lose_days=0):
        for x in range(len(df['date'])):
            if x == len(df['date']) - 1:
                percentage_change_in_net_liquidation = (df['NetLiquidation'][x] - df['NetLiquidation'][pos]) / df['NetLiquidation'][pos]
                # print(range_df['NetLiquidation'][x], 1)
                # print(pos)
                # print(percentage_change_in_net_liquidation, 1)
                check = True
            elif df['date'][x] != df['date'][x + 1]:
                percentage_change_in_net_liquidation = (df['NetLiquidation'][x] - df['NetLiquidation'][pos]) / df['NetLiquidation'][pos]
                # print(range_df['NetLiquidation'][x], 2)
                # print(pos)
                # print(percentage_change_in_net_liquidation, 2)
                check = True
            if check is True:
                if x == pos:  # for this simple backtest only
                    sum_of_percentage_increased += df['NetLiquidation'][x] / 10000000
                    number_of_win_days += 1
                elif percentage_change_in_net_liquidation > 0:
                    sum_of_percentage_increased += percentage_change_in_net_liquidation
                    number_of_win_days += 1
                elif percentage_change_in_net_liquidation < 0:
                    sum_of_percentage_decreased += percentage_change_in_net_liquidation
                    number_of_lose_days += 1
                pos = x + 1
            check = False
        if number_of_win_days == 0:
            average_win_day = 0
        else:
            average_win_day = sum_of_percentage_increased / number_of_win_days
        if number_of_lose_days == 0:
            average_lose_day = 0
        else:
            average_lose_day = abs(sum_of_percentage_decreased / number_of_lose_days)
        return [average_win_day, average_lose_day]

def main():
    engine = sim_data_io_engine.offline_engine('/Applications/IndexTrading/user_id_0/backtest/backtest_rebalance_margin_wif_max_drawdown_control_0/run_data')

    my_stat_engine = statistic_engine_3(engine)
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
    #print(my_stat_engine.get_rolling_return_by_range(range,'0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_',"10y"))
#print(my_stat_engine.get_average_win_by_period("2022-05-26", '5y', '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_average_win_day_by_period("2022-04-28", '1m', '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_average_win_day_by_range(["2022-03-31", "2022-4-28"], '0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_average_win_day_ytd('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_average_win_day_inception('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    #print(my_stat_engine.get_average_win_day_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    print(my_stat_engine.get_profit_loss_ratio_data('0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_'))
    # print(my_stat_engine.get_average_win_day_by_period("2022-04-28", '1m','my_test'))
    # print(my_stat_engine.get_average_win_day_by_range(["2022-03-31", "2022-4-28"],'my_test'))
    # print(my_stat_engine.get_average_win_day_ytd('my_test'))
    # print(my_stat_engine.get_average_win_day_inception('my_test'))
    # print(my_stat_engine.get_average_win_day_data('my_test'))
    # print(my_stat_engine.get_profit_loss_ratio_by_period("2022-04-28", '1m', 'my_test'))
    # print(my_stat_engine.get_profit_loss_ratio_by_range(["2022-03-31", "2022-4-28"], 'my_test'))
    # print(my_stat_engine.get_profit_loss_ratio_ytd('my_test'))
    # print(my_stat_engine.get_profit_loss_ratio_inception('my_test'))
    # print(my_stat_engine.get_profit_loss_ratio_data('my_test'))

if __name__ == "__main__":
    main()
