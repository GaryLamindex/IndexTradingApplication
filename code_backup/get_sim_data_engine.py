import pandas as pd
from decimal import Decimal
import json
import datetime as dt

# for handle decimal serialization in JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

# some useful utilities
class utils:
    # for the following functions, assume that the the parameter "result" is a db table in json format
    def json_to_dataframe(self,result):
        data_string = json.dumps(result,cls=DecimalEncoder)
        df = pd.read_json(data_string,orient='records')
        return df

    def json_to_csv(self,result):
        # the default name for the output file is result.csv
        df = self.query_result_to_dataframe(result)
        df.to_csv("result.csv")

    def is_leap_year(self,year):
        if((year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)):
            return True
        else:
            return False

    # get the lower bound (timestamp) with the input timestamp and the lookback period
    # supported lookback_period: 1d, 1m, 6m, 1y, 5y
    def return_lower_bound_from_timestamp(self,timestamp,lookback_period):
        day_30_months = [4,6,9,11]
        day_31_months = [1,3,5,7,8,10,12]

        # utc_dt = dt.datetime.utcfromtimestamp(timestamp)
        year = timestamp.year
        month = timestamp.month
        day = timestamp.day

        if lookback_period == "1d":
            new_Date = f'{year}-{month}-{day} 00:00:00'
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
        
        return new_datetime_obj

    # return a tuple
    # trading data of the specified day must lie between the returned tuple
    # summer: 1330-1959, winter: 1430-2059
    def get_timestamp(self,date_string): # assume we input one day (e.g. 2017-10-20)
        # assuming that we will only receive a date, without any time data (i.e. H,M,S)
        start_datetime_obj = dt.datetime.strptime(date_string + " 13:30:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
        end_datetime_obj = dt.datetime.strptime(date_string + " 20:59:59", "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)

        result = (start_datetime_obj,end_datetime_obj)

        return result

class get_sim_data_engine:
    input_json_table = None
    my_utils = None
    table_df = None
    
    def __init__(self,table):
        self.input_json_table = table
        self.my_utils = utils()
        self.table_df = self.my_utils.json_to_dataframe(self.input_json_table)

    def get_full_df(self,**kwargs):
        return self.table_df[self.table_df['spec'] == list(kwargs.values())[0]] if (len(kwargs) == 1) else self.table_df

    def get_data(self,date,lookback_period,**kwargs):
        # need not check kwargs, since already handled in new_data_engine
        # there are totally 5 cases for the lookback period
        # "1d", "1m", "6m", "1y", "5y"

        # similarities: ending timestamp and kwargs (spec) handling
        # ending timestamp
        ending_timestamp = self.my_utils.get_timestamp(date)[1]

        # kwargs
        processing_df = self.table_df[self.table_df['spec'] == list(kwargs.values())[0]] if (len(kwargs) == 1) else self.table_df

        # difference: starting timestamp
        if lookback_period == "1d":
            # starting_timestamp = self.my_utils.get_timestamp(date)[0] 
            starting_timestamp = self.my_utils.return_lower_bound_from_timestamp(ending_timestamp,"1d")
        elif lookback_period == "1m":
            starting_timestamp = self.my_utils.return_lower_bound_from_timestamp(ending_timestamp,"1m")
        elif lookback_period == "6m":
            starting_timestamp = self.my_utils.return_lower_bound_from_timestamp(ending_timestamp,"6m")
        elif lookback_period == "1y":
            starting_timestamp = self.my_utils.return_lower_bound_from_timestamp(ending_timestamp,"1y")
        elif lookback_period == "5y":
            starting_timestamp = self.my_utils.return_lower_bound_from_timestamp(ending_timestamp,"5y")

        # similarities: slicing the data frame according to the starting_timestamp and the ending_timestamp
        processing_df = processing_df[(processing_df['timestamp'].dt.tz_localize(dt.timezone.utc) >= starting_timestamp) & (processing_df['timestamp'].dt.tz_localize(dt.timezone.utc) <= ending_timestamp)]

        processing_df.reset_index(inplace=True, drop=True)

        return processing_df

    def get_data_range(self,range,**kwargs):
        if (range[0] == range[1]):
            return self.get_data(range[0],"1d")
        else:
            upper = self.my_utils.get_timestamp(range[1])[1]
            lower = self.my_utils.get_timestamp(range[0])[0]

        # kwargs
        processing_df = self.table_df[self.table_df['spec'] == list(kwargs.values())[0]] if (len(kwargs) == 1) else self.table_df

        # slicing
        processing_df = processing_df[(processing_df['timestamp'].dt.tz_localize(dt.timezone.utc) >= lower) & (processing_df['timestamp'].dt.tz_localize(dt.timezone.utc) <= upper)]

        processing_df.reset_index(inplace=True, drop=True)

        return processing_df

    def get_last_n_row(self,n,**kwargs):
        processing_df = self.table_df[self.table_df['spec'] == list(kwargs.values())[0]] if (len(kwargs) == 1) else self.table_df

        processing_df = processing_df.groupby('spec').tail(n)

        return processing_df.reset_index(drop=True)

    def minute_frame_to_daily_frame(self,df):
        temp_df = df.copy()
        temp_df['temp_date'] = temp_df['timestamp'].dt.date
        grouped_df = temp_df.groupby('temp_date')
        result = grouped_df.first().reset_index(drop=True)
        return result


def main():
    df = pd.read_pickle("df.txt")
    my_utils = utils()

if __name__ == "__main__":
    main()