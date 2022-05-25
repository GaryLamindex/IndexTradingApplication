import os
import sys
import pandas as pd
from decimal import Decimal
import datetime as dt
import json
import pathlib
import csv

# grab the dynamo_db_engine class
script_dir = os.path.dirname(__file__)
db_engine_dir = os.path.join(script_dir, '..', 'aws_engine')
sys.path.append(db_engine_dir)

from engine.aws_engine.dynamo_db_engine import dynamo_db_engine as db_engine

def check_kwargs(kwargs):
    if (len(kwargs) > 1 or len(kwargs) == 1 and list(kwargs.keys())[0] != "spec"):
        sys.exit("Wrong parameter")

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
        df = pd.read_json(data_string,orient='records', convert_dates=False)
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
    # supported lookback_period: 1d, 1m, 6m, 1y, 3y, 5y
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
        elif lookback_period == "3y":
            if month == 2 and day == 29:
                new_Date = f'{year - 3}-02-28 00:00:00'
            else:
                new_Date = f'{year - 3}-{month}-{day} 00:00:00'
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

class online_engine:

    db_engine = None
    full_table = None # json format
    table_name = ""
    utils = None # utils object
    full_table_df = None # dataframe
    cache_df = {}

    def __init__(self,table_info):
        self.table_name = table_info.get("mode")+"_"+table_info.get("strategy_name")+"_"+str(table_info.get("user_id"))

        self.db_engine = db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
        self.full_table = self.db_engine.get_whole_table(self.table_name)
        self.utils = utils()
        self.full_table_df = self.utils.json_to_dataframe(self.full_table)


    # handle the real time update of the table
    # decorator function
    def update(self):
        """
        This function will be called in the get_data functions
        """
        # update the db_engine first
        self.db_engine = db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
        # get the current full table without slicing
        current_df = self.get_full_df()
        current_max_timestamp = current_df['timestamp'].max()
        unique_spec = current_df['spec'].unique()
        for spec in unique_spec:
            condition = ["timestamp",">",int(current_max_timestamp.value/1000000000)]
            new_data = self.db_engine.query_all_by_condition(self.table_name,spec,condition)
            to_be_appended = self.utils.json_to_dataframe(new_data)
            current_df = pd.concat([to_be_appended,current_df],ignore_index=False)

        # sort the new dataframe
        current_df.sort_values(['timestamp'],ascending=True,inplace=True,kind='stable')
        current_df.sort_values(['spec'],ascending=True,inplace=True,kind='stable')

        self.full_table_df = current_df.reset_index(drop=True)

    def get_full_df(self,**kwargs):
        # checking kwargs
        check_kwargs(kwargs)
        if (len(kwargs) == 1):
            return self.full_table_df[self.full_table_df['spec'] == list(kwargs.values())[0]]
        else:
            return self.full_table_df

    # assumption: the table will always follow the format which
    # the partition key is "spec" with data format "String"
    # the sort key is "timestamp" with data format "Number"
    def delete_and_init(self):
        """
        Example: suppose you created an sim_data_io_engine object called "engine"
        use this function by simply calling enigne.delete_and_init()
        """
        key_attribute = {"partition_key": {"name": "spec", "keytype": "S"}, "sort_key": {"name": "timestamp", "keytype": "N"}}
        self.db_engine.dynamodb.Table(self.table_name).delete()
        # the deletion process is very slow
        while True:
            try:
                self.db_engine.create_table(self.table_name,key_attribute)
                message = ""
            except Exception as e:
                message = str(e)
            if (message == ""):
                break

    # For the following functions
    # use of **kwargs to indicate whether user want to specify a particular spec
    # accept ONE (or none) keyword: "spec"
    # Examples:

    # there are totally 5 cases for the lookback period
    # "1d", "1m", "6m", "1y", "5y"
    def get_data_by_period(self,date,lookback_period,**kwargs):
        """
        "lookback_period" parameter: "1d", "1m", "6m", "1y", "3y", "5y"
        Example 1: get_data_by_period("2017-11-01","1d")
        The above function call does NOT include kwarg, then will return a resulting df with ALL specs
        Example 2: get_data_by_period("2017-11-30","1m",spec="0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0")
        The above function call INCLUDES kwargs, then will return a resulting df with ONLY the specific spec "0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0"
        """
        # checking kwargs
        check_kwargs(kwargs)
        # make the table up to date
        self.update()

        # similarities: ending timestamp and kwargs (spec) handling
        # ending timestamp
        ending_timestamp = self.utils.get_timestamp(date)[1]

        # kwargs
        # period_df is the sliced dataframe of the specified period
        data_by_period_df = self.full_table_df[self.full_table_df['spec'] == list(kwargs.values())[0]] if (len(kwargs) == 1) else self.full_table_df

        # difference: starting timestamp
        if lookback_period == "1d":
            # starting_timestamp = self.utils.get_timestamp(date)[0]
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"1d")
        elif lookback_period == "1m":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"1m")
        elif lookback_period == "6m":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"6m")
        elif lookback_period == "1y":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"1y")
        elif lookback_period == "3y":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"3y")
        elif lookback_period == "5y":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"5y")

        # similarities: slicing the data frame according to the starting_timestamp and the ending_timestamp
        data_by_period_df = data_by_period_df[(data_by_period_df['timestamp'].dt.tz_localize(dt.timezone.utc) >= starting_timestamp) & (data_by_period_df['timestamp'].dt.tz_localize(dt.timezone.utc) <= ending_timestamp)]

        data_by_period_df.reset_index(inplace=True,drop=True)

        return data_by_period_df

    def get_data_by_range(self,range,**kwargs):
        """
        for the parameter "range", the format is something like ["2017-10-27","2017-11-30"]
        range = ["2017-10-27","2017-11-30"]
        Example 1: get_data_by_range(range) -> return all range data with all specs
        Example 2: get_data_by_range(range,spec="0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0") -> return all range data with specific specs
        """
        # checking kwargs
        check_kwargs(kwargs)
        # make the table up to date
        self.update()
        if (range[0] == range[1]):
            return self.get_data(range[0],"1d")
        else:
            upper = self.utils.get_timestamp(range[1])[1]
            lower = self.utils.get_timestamp(range[0])[0]

        # kwargs
        data_by_range_df = self.full_table_df[self.full_table_df['spec']== list(kwargs.values())[0]] if (len(kwargs) == 1) else self.full_table_df

        # slicing
        data_by_range_df = data_by_range_df[(data_by_range_df['timestamp'].dt.tz_localize(dt.timezone.utc) >= lower) & (data_by_range_df['timestamp'].dt.tz_localize(dt.timezone.utc) <= upper)]

        data_by_range_df.reset_index(inplace=True,drop=True)

        return data_by_range_df

    def get_last_n_row_of_specs(self,n,**kwargs):
        """
        Example: get_last_n_row(5) -> return all last n data with all specs
        Example: get_last_n_row(10,spec="0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0") -> return all last n data with specifics specs
        """
        # checking kwargs
        check_kwargs(kwargs)
        # make the table up to date
        self.update()

        last_n_data_df = self.full_table_df[self.full_table_df['spce'] == list(kwargs.values())[0]] if (len(kwargs) == 1) else self.full_table_df

        last_n_data_df = last_n_data_df.groupby('spec').tail(n)

        return last_n_data_df.reset_index(drop=True)

    """to be put in utils"""
    def minute_frame_to_daily_frame(self,df):
        temporary_df = df.copy()
        temporary_df['temp_date'] = temporary_df['timestamp'].dt.date
        gropued_df = temporary_df.groupby('temp_date')
        result = gropued_df.first().reset_index(drop=True)
        return result

    # expected data format: array of dictionary
    # example: [{"spec":"spec1","timestamp":1234567},{"spec":"spec1","timestamp":23456789}]
    def upload_sim_data(self,items):
        """
        Example:
        items = [{"spec":"spec1","timestamp":1234567},{"spec":"spec2","timestamp":23456789}]
        engine.upload_sim_data("backtest_rebalance_margin_wif_max_drawdown_control_0",items)
        """
        table = self.db_engine.dynamodb.Table(self.table_name)
        for item_dict in items:
            try:
                table.put_item(Item=item_dict)
            except Exception as e:
                print("Item -",item_dict)
                print("Error message-", e.response['Error']['Message'])

class offline_engine:
    table_name = ""
    utils = None  # utils object
    # full_table_df = None  # dataframe
    filepath = "" # the file path for reaching the data file
    cache_full_df = {}
    def __init__(self, run_file_dir):
        # strategy_name = table_info.get("strategy_name")
        # mode = table_info.get("mode")
        # user_id = str(table_info.get("user_id"))
        # self.table_name = f"{mode}_{strategy_name}_{user_id}"
        self.utils = utils()
        # search the file path of the data
        self.filepath = run_file_dir

        # getting a list of files in the specified directory
        # files = os.listdir(self.filepath)
        # df_list = []
        # for file in files:
        #     with open(f'{self.filepath}\\{file}') as f:
        #         df = pd.read_csv(f)
        #         df_list.append(df)

        # if len(df_list) > 0:
        #     # concatenate all the dataframes into full_table_df
        #     self.full_table_df = pd.concat(df_list).reset_index(drop=True)

    # def update(self):
    #     """
    #     This function will be called in the get_data functions
    #     """
    #     self.full_table_df = None
    #     files = os.listdir(self.filepath)
    #     df_list = []
    #     for file in files:
    #         with open(f'{self.filepath}\\{file}') as f:
    #             df = pd.read_csv(f)
    #             df_list.append(df)
    #     if len(df_list) > 0:
    #         # concatenate all the dataframes into full_table_df
    #         self.full_table_df = pd.concat(df_list).reset_index(drop=True)

    # def get_full_df(self,**kwargs):
    #     # checking kwargs
    #     check_kwargs(kwargs)

    #     return self.full_table_df[self.full_table_df['spec'] == list(kwargs.values())[0]] if (len(kwargs) == 1) else self.full_table_df

    # def delete_and_init(self,spec):
    #     # check if the file exists first
    #     if (f'{spec}.csv' in os.listdir(self.filepath)):
    #         os.remove(f'{self.filepath}\\{spec}.csv')
    #     # wrtie a new file with header only
    #     with open(f'{self.filepath}\\{spec}.csv','a',newline='') as f:
    #         writer = csv.writer(f)
    #         writer.writerow(["spec","timestamp"])

    def get_full_df(self,file_name):
        print(f"{self.filepath}/{file_name}.csv")
        if file_name in self.cache_full_df:
            return self.cache_full_df[file_name]
        else:
            print("getting full_df")
            full_df = pd.read_csv(f"{self.filepath}/{file_name}.csv", low_memory=False)
            self.cache_full_df = {file_name:full_df}
            print("getting full_df: done")
            print(full_df)
            # with open(f"{self.filepath}/{spec}.csv","a+") as f:
            #     full_df = pd.read_csv(f, engine="python")
        return full_df

    def get_data_by_period(self,date,lookback_period,file_name):
        """
        "lookback_period" parameter: "1d", "1m", "6m", "1y", "3y", "5y"
        Example: get_data_by_period("2017-11-30","1m","0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0")
        return a single df
        """
        # similarities: ending timestamp and kwargs (spec) handling
        # ending timestamp
        ending_timestamp = self.utils.get_timestamp(date)[1]

        # get the df by reading the corr. csv
        # data_by_period_df = pd.read_csv(f"{self.filepath}/{spec}.csv", low_memory=False)
        data_by_period_df = self.get_full_df(file_name)
        # difference: starting timestamp
        if lookback_period == "1d":
            # starting_timestamp = self.utils.get_timestamp(date)[0]
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"1d")
        elif lookback_period == "1m":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"1m")
        elif lookback_period == "6m":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"6m")
        elif lookback_period == "1y":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"1y")
        elif lookback_period == "3y":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"3y")
        elif lookback_period == "5y":
            starting_timestamp = self.utils.return_lower_bound_from_timestamp(ending_timestamp,"5y")

        # similarities: slicing the data frame according to the starting_timestamp and the ending_timestamp
        data_by_period_df = data_by_period_df[(data_by_period_df['timestamp'] >= starting_timestamp.timestamp()) & (data_by_period_df['timestamp'] <= ending_timestamp.timestamp())]

        data_by_period_df.reset_index(inplace=True,drop=True)

        return data_by_period_df

    def get_data_by_range(self,range, file_name):
        """
        for the parameter "range", the format is something like ["2017-10-27","2017-11-30"]
        range = ["2017-10-27","2017-11-30"]
        Example : get_data_by_range(range,"0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0") -> return the range data of the specified spec
        """
        if (range[0] == range[1]):
            return self.get_data_by_period(range[0],"1d",file_name)
        else:
            upper = self.utils.get_timestamp(range[1])[1]
            lower = self.utils.get_timestamp(range[0])[0]

        # get the df by reading the corr. csv
        # data_by_range_df = pd.read_csv(f"{self.filepath}/{spec}.csv", low_memory=False)
        data_by_range_df = self.get_full_df(file_name)
        # slicing
        data_by_range_df = data_by_range_df[(data_by_range_df['timestamp'] >= lower.timestamp()) & (data_by_range_df['timestamp'] <= upper.timestamp())]

        data_by_range_df.reset_index(inplace=True,drop=True)

        return data_by_range_df

    def get_last_n_row_of_specs(self,n, file_name):
        """
        Example: get_last_n_row(10,"0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0") -> return last n data of hte specified spec
        """

        # with open(f"{self.filepath}/{spec}.csv","a+") as f:
        #     last_n_data_df = pd.read_csv(f)
        full_df = self.get_full_df(file_name)
        last_n_data_df = full_df.tail(n)

        return last_n_data_df.reset_index(drop=True)

    # write the corresponding data to a particular file
    def upload_full_sim_data(self,spec,items):
        """
        Example:
        items = [{"timestamp":1234567},{"timestamp":23456789}]
        engine.upload_sim_data("spec1",items)
        """
        spec_str = ""
        for k, v in spec.items():
            spec_str = f"{spec_str}{str(v)}_{str(k)}_"
        print(spec_str)

        for item in items:
            item["spec"] = spec_str # add a element of "spec" to the item
            item['timestamp'] = int(item['timestamp'])
        item_df = self.utils.json_to_dataframe(items)
        # open the corresponding csv files
        # write the corresponding sliced data to the csv file

        # the case which the file exist
        if (f'{spec_str}.csv' in os.listdir(self.filepath)):
            with open(f'{self.filepath}\\{spec_str}.csv','a',newline='') as f:
                print(f.name)
                item_df.to_csv(f,mode='a',index=False,header=False)
        # the case which the file does not exist
        else:
            with open(f'{self.filepath}\\{spec_str}.csv','a',newline='') as f:
                print(f.name)
                item_df.to_csv(f,mode='w',index=False,header=True)

    def upload_single_sim_data(self,spec_str,item):
        """
        Example:
        items = [{"timestamp":1234567},{"timestamp":23456789}]
        engine.upload_sim_data("spec1",items)
        """
        item["spec"] = spec_str # add a element of "spec" to the item
        item['timestamp'] = int(item['timestamp'])
        items = []
        items.append(item)
        item_df = self.utils.json_to_dataframe(items)
        # open the corresponding csv files
        # write the corresponding sliced data to the csv file

        # the case which the file exist
        if (f'{spec_str}.csv' in os.listdir(self.filepath)):
            with open(f'{self.filepath}\\{spec_str}.csv','a',newline='') as f:
                print(f.name)
                item_df.to_csv(f,mode='a',index=False,header=False)
        # the case which the file does not exist
        else:
            with open(f'{self.filepath}\\{spec_str}.csv','a',newline='') as f:
                print(f.name)
                item_df.to_csv(f,mode='w',index=False,header=True)

def main():
    # table_info = {"mode":"backtest_rebalance","strategy_name":"margin_wif_max_drawdown_control","user_id":0}
    # engine = online_engine(table_info)
    # min_df = engine.get_data_by_period("2017-11-30","1m")
    # print(min_df)
    haha = offline_engine({"mode":"backtest","strategy_name":"rebalance_margin_wif_max_drawdown_control","user_id":0})
    print(haha.filepath)
    # print(haha.get_full_df(spec="0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0"))
    # range = ["2017-10-27","2017-11-30"]
    # print(haha.get_data_by_range(range)['NetLiquidation(Day Start)'])
    # print(haha.get_last_n_row_of_specs(5))
    # hehe = online_engine({"mode":"backtest_rebalance","strategy_name":"margin_wif_max_drawdown_control","user_id":0})
    # print(haha.get_data_by_range(range)['NetLiquidation(Day Start)'])
    # print(hehe.get_last_n_row_of_specs(5))
    # items = [{"timestamp":1234567},{"timestamp":23456789}]
    # haha.upload_sim_data("spec1",items)
    haha.delete_and_init("spec1")
if __name__ == "__main__":
    main()