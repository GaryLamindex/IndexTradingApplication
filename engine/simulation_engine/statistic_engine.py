import pandas as pd
import numpy as np
import os
import sys
import datetime as dt

# 5-yr average
from engine.simulation_engine import sim_data_io_engine

RISK_FREE_RATE = 0.0127
# equivalent rate for 60s
EQV_RISK_FREE_RATE = (1 + RISK_FREE_RATE) ** (60 / (365 * 24 * 60 * 60)) - 1

# a global function for risk free rate processing
# support 2 possible kw arguments: "timeframe" & "range"
# "timeframe" only support "1d", "1m", "6m", "1y" & "5y"
def eq_riskfree_rate(**kwargs):
    # check usage 
    if (len(kwargs) != 1 or (list(kwargs.keys())[0] != "timeframe" and list(kwargs.keys())[0] != "range")):
        sys.exit("Wrong parameter")
    if (list(kwargs.keys())[0] == "timeframe"):
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
    elif (list(kwargs.keys())[0] == "range"):
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

    def __init__(self,data_engine):
        self.data_engine = data_engine
    """
    For the name of the columns, may need to change mannually (e.g., NetLiquidation those)
    """
    # functions about return
    def get_return_by_period(self,date,lookback_period,file_name):
        # there are totally 5 cases for the lookback period 
        # "1d", "1m", "6m", "1y", "3y" "5y"

        # get the sliced frame for the given lookback_period
        if lookback_period == "1d":
            data_period_df = self.data_engine.get_data_by_period(date,"1d",file_name)
        elif lookback_period == "1m":
            data_period_df = self.data_engine.get_data_by_period(date,"1m",file_name)
        elif lookback_period == "6m":
            data_period_df = self.data_engine.get_data_by_period(date,"6m",file_name)
        elif lookback_period == "1y":
            data_period_df = self.data_engine.get_data_by_period(date,"1y",file_name)
        elif lookback_period == "3y":
            data_period_df = self.data_engine.get_data_by_period(date,"3y",file_name)
        elif lookback_period == "5y":
            data_period_df = self.data_engine.get_data_by_period(date,"5y",file_name)
        

        starting_net_liquidity = data_period_df.loc[data_period_df['timestamp'] == data_period_df['timestamp'].min()]['NetLiquidation'].values[0]
        ending_net_liquidity = data_period_df.loc[data_period_df['timestamp'] == data_period_df['timestamp'].max()]['NetLiquidation'].values[0]

        return (ending_net_liquidity - starting_net_liquidity) / starting_net_liquidity

    def get_return_by_range(self,range,file_name):
        # get the sliced data for the given range
        print("range")
        print(range)
        range_df = self.data_engine.get_data_by_range(range,file_name)
        print("range_df")
        print(range_df)
        print()

        starting_net_liquidity = range_df.loc[range_df['timestamp'] == range_df['timestamp'].min()]['NetLiquidation'].values[0]
        ending_net_liquidity = range_df.loc[range_df['timestamp'] == range_df['timestamp'].max()]['NetLiquidation'].values[0]

        return (ending_net_liquidity - starting_net_liquidity) / starting_net_liquidity

    def get_return_inception(self,file_name):
        print("get_return_inception")
        inception_df = self.data_engine.get_full_df(file_name)

        starting_net_liquidity = inception_df.loc[inception_df['timestamp'] == inception_df['timestamp'].min()]['NetLiquidation'].values[0]
        ending_net_liquidity = inception_df.loc[inception_df['timestamp'] == inception_df['timestamp'].max()]['NetLiquidation'].values[0]
        print(f"starting_net_liquidity:{starting_net_liquidity}; ending_net_liquidity:{ending_net_liquidity}")
        return (ending_net_liquidity - starting_net_liquidity) / starting_net_liquidity

    def get_return_ytd(self,file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01",f"{year}-{month}-{day}"]
        return self.get_return_by_range(range,file_name)

    # return a dictionary of all return info (ytd, 1y, 3y, 5y and inception)
    def get_return_data(self,file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        
        return_dict = {}
        return_dict["ytd"] = self.get_return_ytd(file_name)
        return_dict["1y"] = self.get_return_by_period(day_string,"1y",file_name)
        return_dict["3y"] = self.get_return_by_period(day_string, "3y",file_name)
        return_dict["5y"] = self.get_return_by_period(day_string, "5y",file_name)
        return_dict["inception"] = self.get_return_inception(file_name)

        return return_dict

    # simply use the discrete calculation for sharpe
    # annualize all the results
    def get_sharpe_by_period(self,date,lookback_period,file_name):
        # there are totally 5 cases for the lookback period 
        # "1d", "1m", "6m", "1y", "3y", "5y"

        # get the sliced frame for the given lookback_period
        if lookback_period == "1d":
            data_period_df = self.data_engine.get_data_by_period(date,"1d",file_name)
        elif lookback_period == "1m":
            data_period_df = self.data_engine.get_data_by_period(date,"1m",file_name)
        elif lookback_period == "6m":
            data_period_df = self.data_engine.get_data_by_period(date,"6m",file_name)
        elif lookback_period == "1y":
            data_period_df = self.data_engine.get_data_by_period(date,"1y",file_name)
        elif lookback_period == "3y":
            data_period_df = self.data_engine.get_data_by_period(date,"3y",file_name)
        elif lookback_period == "5y":
            data_period_df = self.data_engine.get_data_by_period(date,"5y",file_name)

        multiplier = {"1d": 365**0.5, "1m":12**0.5, "6m":2**0.5, "1y":1, "3y":1/(3**0.5), "5y":1/(5**0.5)}

        ending_nlv = data_period_df['NetLiquidation']
        return_col = ending_nlv.pct_change().dropna()
        avg_period_return = np.array(return_col).mean()
        period_return_std = np.array(return_col).std()
        items = data_period_df.shape[0]
        return (items**0.5) * multiplier[lookback_period] * (avg_period_return - EQV_RISK_FREE_RATE) / period_return_std

    def get_sharpe_by_range(self,range,file_name):
        print("get_sharpe_by_range")
        range_df = self.data_engine.get_data_by_range(range,file_name)

        no_of_days = (pd.to_datetime(range[1], format="%Y-%m-%d") - pd.to_datetime(range[0], format="%Y-%m-%d")).days + 1

        multiplier = (365/no_of_days) ** 0.5

        ending_nlv = range_df['NetLiquidation']
        return_col = ending_nlv.pct_change().dropna()
        avg_period_return = np.array(return_col).mean()
        period_return_std = np.array(return_col).std()
        items = range_df.shape[0]
        return (items**0.5) * multiplier * (avg_period_return - EQV_RISK_FREE_RATE) / period_return_std


    def get_sharpe_inception(self,file_name):
        inception_df = self.data_engine.get_full_df(file_name)

        # the case of a specified spec
        if isinstance(self.data_engine, sim_data_io_engine.online_engine):
            date_column = inception_df['timestamp'].dt.date # since online df returns timestamp data type
            no_of_days = (date_column.max() - date_column.min()).days + 1
        elif isinstance(self.data_engine, sim_data_io_engine.offline_engine):
            date_column = inception_df['timestamp'] # since offline df returns integer data type
            no_of_days = (date_column.max() - date_column.min()) / (24*60*60) + 1

        multiplier = (365/no_of_days) ** 0.5

        ending_nlv = inception_df['NetLiquidation']
        return_col = ending_nlv.pct_change().dropna()
        avg_period_return = np.array(return_col).mean()
        period_return_std = np.array(return_col).std()
        items = inception_df.shape[0]
        return (items**0.5) * multiplier * (avg_period_return - EQV_RISK_FREE_RATE) / period_return_std
    
    def get_sharpe_ytd(self,file_name):
        print("get_sharpe_ytd")
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01",f"{year}-{month}-{day}"]
        return self.get_sharpe_by_range(range,file_name)

    # return a dictionary of all shpare info (ytd, 1y, 3y, 5y and inception)
    def get_sharpe_data(self,file_name):
        print(f"get_sharpe_data:{file_name}")
        sharpe_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        
        sharpe_dict["ytd"] = self.get_sharpe_ytd(file_name)
        sharpe_dict["1y"] = self.get_sharpe_by_period(day_string,"1y",file_name)
        sharpe_dict["3y"] = self.get_sharpe_by_period(day_string, "3y",file_name)
        sharpe_dict["5y"] = self.get_sharpe_by_period(day_string, "5y",file_name)
        sharpe_dict["inception"] = self.get_sharpe_inception(file_name)

        return sharpe_dict

    def get_max_drawdown_by_period(self,date,lookback_period,file_name):
        # there are totally 5 cases for the lookback period 
        # "1d", "1m", "6m", "1y", "3y" "5y"

        # get the sliced frame for the given lookback_period
        if lookback_period == "1d":
            data_period_df = self.data_engine.get_data_by_period(date,"1d",file_name)
        elif lookback_period == "1m":
            data_period_df = self.data_engine.get_data_by_period(date,"1m",file_name)
        elif lookback_period == "6m":
            data_period_df = self.data_engine.get_data_by_period(date,"6m",file_name)
        elif lookback_period == "1y":
            data_period_df = self.data_engine.get_data_by_period(date,"1y",file_name)
        elif lookback_period == "3y":
            data_period_df = self.data_engine.get_data_by_period(date,"3y",file_name)
        elif lookback_period == "5y":
            data_period_df = self.data_engine.get_data_by_period(date,"5y",file_name)

        rolling_max = data_period_df['NetLiquidation'].cummax()
        item_drawdown = data_period_df['NetLiquidation'] / rolling_max - 1
        return item_drawdown.min()
    
    def get_max_drawdown_by_range(self,range,file_name):
        # get the sliced data for the given range
        print("range")
        print(range)
        range_df = self.data_engine.get_data_by_range(range,file_name)
        print("range_df")
        print(range_df)
        print()

        rolling_max = range_df['NetLiquidation'].cummax()
        item_drawdown = range_df['NetLiquidation'] / rolling_max - 1
        return item_drawdown.min()

    def get_max_drawdown_inception(self,file_name):
        inception_df = self.data_engine.get_full_df(file_name)

        rolling_max = inception_df['NetLiquidation'].cummax()
        item_drawdown = inception_df['NetLiquidation'] / rolling_max - 1
        return item_drawdown.min()

    def get_max_drawdown_ytd(self,file_name):
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        range = [f"{year}-01-01",f"{year}-{month}-{day}"]

        return self.get_max_drawdown_by_range(range,file_name)

    def get_max_drawdown_data(self,file_name):
        drawdown_dict = {}
        full_df = self.data_engine.get_full_df(file_name)
        last_day = dt.datetime.fromtimestamp(full_df['timestamp'].max())
        year = last_day.year
        month = last_day.month
        day = last_day.day
        day_string = f"{year}-{month}-{day}"
        
        drawdown_dict["ytd"] = self.get_max_drawdown_ytd(file_name)
        drawdown_dict["1y"] = self.get_max_drawdown_by_period(day_string,"1y",file_name)
        drawdown_dict["3y"] = self.get_max_drawdown_by_period(day_string, "3y",file_name)
        drawdown_dict["5y"] = self.get_max_drawdown_by_period(day_string, "5y",file_name)
        drawdown_dict["inception"] = self.get_max_drawdown_inception(file_name)

        return drawdown_dict

def main():
    # new_data_API = sim_data_io_engine.offline_engine({"mode":"backtest_rebalance","strategy_name":"margin_wif_max_drawdown_control","user_id":0})
    # my_stat_engine = statistic_engine(new_data_API)
    # print(isinstance(new_data_API,sim_data_io_engine.offline_engine))
    # range = ("2017-10-30","2017-11-30")
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
    with open("C:\\dynamodb\\dynamodb_related\\pythonProject\\algo\\rebalance_margin_wif_max_drawdown_control\\backtest\\backtest_data\\backtest_rebalance_margin_wif_max_drawdown_control_0\\0.038_rebalance_margin_0.01_maintain_margin_0.001_max_drawdown_ratio_5.0_purchase_exliq_.csv",'r') as f:
        df = pd.read_csv(f)
    print(df)

if __name__ == "__main__":
    main()