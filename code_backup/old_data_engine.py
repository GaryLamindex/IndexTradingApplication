# handling imports
import os
import sys
import datetime as dt
import numpy as np

RISK_FREE_RATE = 0.0127 # 5-yr average

script_dir = os.path.dirname(__file__)
db_engine_dir = os.path.join(script_dir, '..', 'aws_engine')
sys.path.append(db_engine_dir)

import dynamo_db_engine as db_engine
# the following is the utility functions aboue timestamp
# the format for datetime string should look like: "2017-10-20 13:03:03"

class utils(db_engine.utils):
    # get the lower bound (timestamp) with the input timestamp and the lookback period
    # supported lookback_period: 1d, 1m, 6m, 1y, 5y

    def is_leap_year(self,year):
        if((year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)):
            return True
        else:
            return False

    def return_lower_bound_from_timestamp(self,timestamp,lookback_period):
        day_30_months = [4,6,9,11]
        day_31_months = [1,3,5,7,8,10,12]

        utc_dt = dt.datetime.utcfromtimestamp(timestamp)
        year = utc_dt.year
        month = utc_dt.month
        day = utc_dt.day

        if lookback_period == "1d":
            if day == 1 and month == 1:
                new_Date = f'{year - 1}-12-31 00:00:00'
            elif day == 1:
                new_month = 12 if (month - 1 + 12) % 12 == 0 else (month - 1 + 12) % 12
                if new_month in day_30_months:
                    new_Date = f'{year}-{new_month}-30 00:00:00'
                elif new_month in day_31_months:
                    new_Date = f'{year}-{new_month}-31 00:00:00'
                else: # Febuary
                    if self.is_leap_year(year):
                        new_Date = f'{year}-02-29 00:00:00'
                    else:
                        new_Date = f'{year}-02-28 00:00:00'
            else:
                new_Date = f'{year}-{month}-{day- 1} 00:00:00'
        elif lookback_period == "1m":
            if month == 1:
                new_Date = f'{year - 1}-12-{day} 00:00:00'
            elif month == 3: # reach Febuary
                if day not in [29,30,31]:
                    new_Date = f'{year}-02-{day} 00:00:00'
                elif self.is_leap_year(year):
                    new_Date = f'{year}-02-29 00:00:00'
                elif not self.is_leap_year(year):
                    new_Date = f'{year}-02-28 00:00:00'
            else:
                new_month = 12 if (month - 1 + 12) % 12 == 0 else (month - 1 + 12) % 12
                if day != 31:
                    new_Date = f'{year}-{new_month}-{day} 00:00:00'
                elif new_month in day_30_months:
                    new_Date = f'{year}-{new_month}-30 00:00:00'
                elif new_month in day_31_months:
                    new_Date = f'{year}-{new_month}-31 00:00:00'
        elif lookback_period == "6m":
            new_month = 12 if (month - 6 + 12) % 12 == 0 else (month - 6 + 12) % 12
            if month in [1,2,3,4,5,6]:
                if day != 31:
                    new_Date = f'{year - 1}-{new_month}-{day} 00:00:00'
                elif new_month in day_31_months:
                    new_Date = f'{year - 1}-{new_month}-31 00:00:00'
                elif new_month in day_30_months:
                    new_Date = f'{year - 1}-{new_month}-30 00:00:00'
            elif month == 8: # Febuary
                if day not in [29,30,31]:
                    new_Date = f'{year}-02-{day} 00:00:00'
                elif self.is_leap_year(year):
                    new_Date = f'{year}-02-29 00:00:00'
                elif not self.is_leap_year(year):
                    new_Date = f'{year}-02-28 00:00:00'
            else:
                if day != 31:
                    new_Date = f'{year}-{new_month}-{day} 00:00:00'
                elif new_month in day_30_months:
                    new_Date = f'{year}-{new_month}-30 00:00:00'
                elif new_month in day_31_months:
                    new_Date = f'{year}-{new_month}-31 00:00:00'
        elif lookback_period == "1y":
            if month == 2 and day == 29:
                new_Date = f'{year - 1}-02-28 00:00:00'
            else:
                new_Date = f'{year - 1}-{month}-{day} 00:00:00'
        elif lookback_period == "5y":
            if month == 2 and day == 29:
                new_Date = f'{year - 5}-02-28 00:00:00'
            else:
                new_Date = f'{year - 5}-{month}-{day} 00:00:00'
        
        new_datetime_obj = dt.datetime.strptime(new_Date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
        
        return int(dt.datetime.timestamp(new_datetime_obj))

    # return a tuple
    # trading data of the specified day must lie between the returned tuple
    # summer: 1330-1959, winter: 1430-2059
    def get_timestamp(self,date_string): # assume we input one day (e.g. 2017-10-20)
        # assuming that we will only receive a date, without any time data (i.e. H,M,S)
        start_datetime_obj = dt.datetime.strptime(date_string + " 13:30:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
        start_timestamp = int(dt.datetime.timestamp(start_datetime_obj))
        end_datetime_obj = dt.datetime.strptime(date_string + " 20:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
        end_timestamp = int(dt.datetime.timestamp(end_datetime_obj))

        result = (start_timestamp,end_timestamp)

        return result

class data_engine(object):
    # using engine db_engine
    db_engine = None
    engine_utils = None
    my_full_table = None
    my_calculation_engine = None

    def __init__(self,table_name):
        self.db_engine = db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
        self.my_full_table = db_engine.get_whole_table(table_name)
        self.engine_utils = utils()
        # initialize the calculatoin agent

    # use of **kwargs to indicate whether user want to specify a particular set of data (5y) or not
    # 1. Json data
    # note that for the following function, pass in a date and then will look back for the specified range
    def get_data_1d(self,table_name,partition_key_value,date):
        upper = self.engine_utils.get_timestamp(date)[1]
        lower = self.engine_utils.get_timestamp(date)[0]
        range = (lower,upper)
        return self.db_engine.query_all_by_range(table_name,partition_key_value,"timestamp",range)

    def get_data_1m(self,table_name,partition_key_value,date):
        upper = self.engine_utils.get_timestamp(date)[1]
        lower = self.engine_utils.return_lower_bound_from_timestamp(upper,"1m")
        range = (lower,upper)
        return self.db_engine.query_all_by_range(table_name,partition_key_value,"timestamp",range)

    def get_data_6m(self,table_name,partition_key_value,date):
        upper = self.engine_utils.get_timestamp(date)[1]
        lower = self.engine_utils.return_lower_bound_from_timestamp(upper,"6m")
        range = (lower,upper)
        return self.db_engine.query_all_by_range(table_name,partition_key_value,"timestamp",range)

    def get_data_1y(self,table_name,partition_key_value,date):
        upper = self.engine_utils.get_timestamp(date)[1]
        lower = self.engine_utils.return_lower_bound_from_timestamp(upper,"1y")
        range = (lower,upper)
        return self.db_engine.query_all_by_range(table_name,partition_key_value,"timestamp",range)

    def get_data_5y(self,table_name,partition_key_value,date):
        upper = self.engine_utils.get_timestamp(date)[1]
        lower = self.engine_utils.return_lower_bound_from_timestamp(upper,"5y")
        range = (lower,upper)
        return self.db_engine.query_all_by_range(table_name,partition_key_value,"timestamp",range)

    def get_data_range(self,table_name,partition_key_value,range):
        # if you want to specify a day: using the same data in the array, e.g. ["2017-10-20","2017-10-20"]
        # if you want to specify a range: using different data in the array, e.g. ["2017-10-20","2018-10-20"]
        if (range[0] == range[1]): # fetching only a day
            return self.get_data_1d(table_name,partition_key_value,range[0])
        else:
            upper = self.engine_utils.get_timestamp(range[1])[1]
            lower = self.engine_utils.get_timestamp(range[0])[0]
            range = (lower,upper)
            return self.db_engine.query_all_by_range(table_name,partition_key_value,"timestamp",range)

    # 2. Returns
    def get_1_year_return(self,table_name,partition_key_value,lookback_year):
        start = f'{lookback_year}-01-01'
        end = f'{lookback_year}-12-31'
        range = (start,end)
        data = self.get_data_range(table_name,partition_key_value,range)
        data_size = len(data)
        start_net_liquidity = data[0]["NetLiquidation(Day Start)"]
        end_net_liquidity = data[data_size-1]["NetLiquidation(Day End)"]
        return (end_net_liquidity-start_net_liquidity) / start_net_liquidity

    def get_3_year_return(self,table_name,partition_key_value,lookback_year):
        start = f'{lookback_year - 2}-01-01'
        end = f'{lookback_year}-12-31'
        range = (start,end)
        data = self.get_data_range(table_name,partition_key_value,range)
        data_size = len(data)
        start_net_liquidity = data[0]["NetLiquidation(Day Start)"]
        end_net_liquidity = data[data_size-1]["NetLiquidation(Day End)"]
        return (end_net_liquidity-start_net_liquidity) / start_net_liquidity

    def get_5_year_return(self,table_name,partition_key_value,lookback_year):
        start = f'{lookback_year - 4}-01-01'
        end = f'{lookback_year}-12-31'
        range = (start,end)
        data = self.get_data_range(table_name,partition_key_value,range)
        data_size = len(data)
        start_net_liquidity = data[0]["NetLiquidation(Day Start)"]
        end_net_liquidity = data[data_size-1]["NetLiquidation(Day End)"]
        return (end_net_liquidity-start_net_liquidity) / start_net_liquidity

    def get_inception_return(self,table_name,partition_key_value):
        data = self.db_engine.query_all_data(table_name,partition_key_value)
        data_size = len(data)
        start_net_liquidity = data[0]["NetLiquidation(Day Start)"]
        end_net_liquidity = data[data_size-1]["NetLiquidation(Day End)"]
        return (end_net_liquidity-start_net_liquidity) / start_net_liquidity

    def get_range_return(self,table_name,partition_key_value,range):
        # the range should be specified in the format like: ["2017-10-20","2018-01-01"]
        data = self.get_data_range(table_name,partition_key_value,range)
        data_size = len(data)
        start_net_liquidity = data[0]["NetLiquidation(Day Start)"]
        end_net_liquidity = data[data_size-1]["NetLiquidation(Day End)"]
        return (end_net_liquidity-start_net_liquidity) / start_net_liquidity

    # 3. Sharpe
    # taking the risk-free rate as 1.27% (5-yr average)
    def get_1_year_sharpe(self,table_name,partition_key_value,lookback_year):
        start = f'{lookback_year}-01-01'
        end = f'{lookback_year}-12-31'
        range = (start,end)
        data = self.get_data_range(table_name,partition_key_value,range)
        df = self.engine_utils.query_result_to_dataframe(data)
        liquidity_arr = np.array(df["NetLiquidation(Day End)"])
        liquidity_std = np.std(liquidity_arr)
        period_return = self.get_1_year_return(table_name,partition_key_value,lookback_year)

        return (float(period_return) - RISK_FREE_RATE) / liquidity_std

    def get_3_year_sharpe(self,table_name,partition_key_value,lookback_year):
        start = f'{lookback_year - 2}-01-01'
        end = f'{lookback_year}-12-31'
        range = (start,end)
        data = self.get_data_range(table_name,partition_key_value,range)
        df = self.engine_utils.query_result_to_dataframe(data)
        liquidity_arr = np.array(df["NetLiquidation(Day End)"])
        liquidity_std = np.std(liquidity_arr)
        period_return = self.get_3_year_return(table_name,partition_key_value,lookback_year)

        eqv_riskfree_rate = pow((1 + RISK_FREE_RATE), 3) - 1

        return (float(period_return) - eqv_riskfree_rate) / liquidity_std

    def get_5_year_sharpe(self,table_name,partition_key_value,lookback_year):
        start = f'{lookback_year - 4}-01-01'
        end = f'{lookback_year}-12-31'
        range = (start,end)
        data = self.get_data_range(table_name,partition_key_value,range)
        df = self.engine_utils.query_result_to_dataframe(data)
        liquidity_arr = np.array(df["NetLiquidation(Day End)"])
        liquidity_std = np.std(liquidity_arr)
        period_return = self.get_5_year_return(table_name,partition_key_value,lookback_year)

        eqv_riskfree_rate = pow((1 + RISK_FREE_RATE), 5) - 1

        return (float(period_return) - eqv_riskfree_rate) / liquidity_std

    def get_inception_sharpe(self,table_name,partition_key_value):
        data = self.db_engine.query_all_data(table_name,partition_key_value)
        data_size = len(data)
        df = self.engine_utils.query_result_to_dataframe(data)
        liquidity_arr = np.array(df["NetLiquidation(Day End)"])
        liquidity_std = np.std(liquidity_arr)
        period_return = self.get_inception_return(table_name,partition_key_value)

        years_count = float((data[data_size-1]["timestamp"] - data[0]["timestamp"]) / (60 * 60 * 24 * 365))

        eqv_riskfree_rate = pow((1 + RISK_FREE_RATE), years_count) - 1

        return (float(period_return) - eqv_riskfree_rate) / liquidity_std

    def get_range_sharpe(self,table_name,partition_key_value,range):
        # the range should be specified in the format like: ["2017-10-20","2018-01-01"]
        data = self.get_data_range(table_name,partition_key_value,range)
        data_size = len(data)
        
        df = self.engine_utils.query_result_to_dataframe(data)
        liquidity_arr = np.array(df["NetLiquidation(Day End)"])
        liquidity_std = np.std(liquidity_arr)
        period_return = self.get_inception_return(table_name,partition_key_value)

        years_count = float((data[data_size-1]["timestamp"] - data[0]["timestamp"]) / (60 * 60 * 24 * 365))

        eqv_riskfree_rate = pow((1 + RISK_FREE_RATE), years_count) - 1

        return (float(period_return) - eqv_riskfree_rate) / liquidity_std

    # 4. Return jpg


    # 5. Return stats table

def main():
    # my_utils = utils()
    # my_date = "2000-08-30"
    # data = my_utils.get_timestamp(my_date)[0]
    # date = my_utils.return_lower_bound_from_timestamp(data,"6m")
    # print(date)
    my_engine = data_engine()
    range = ("2017-10-20","2017-12-01")
    haha = my_engine.get_range_sharpe("backtest_rebalance_margin_wif_max_drawdown_control_0","0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0",range)
    # haha = my_engine.get_range_return("backtest_rebalance_margin_wif_max_drawdown_control_0","0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0",range)
    # my_utils = utils()
    # df = my_utils.query_result_to_dataframe(haha)
    # print(df['timestamp'])
    # print(len(haha))
    print(haha)


if __name__ == "__main__":
    main()