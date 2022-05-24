import os
import sys
import pandas as pd

# grab the dynamo_db_engine class
script_dir = os.path.dirname(__file__)
db_engine_dir = os.path.join(script_dir, '..', 'aws_engine')
sys.path.append(db_engine_dir)

import dynamo_db_engine as db_engine

# grab the data_engine class
import get_sim_data_engine

def check_kwargs(kwargs):
    if (len(kwargs) > 1 or len(kwargs) == 1 and list(kwargs.keys())[0] != "spec"):
        sys.exit("Wrong parameter")

class get_sim_data_API:
    # private data members # modify later
    db_engine = None
    my_full_table = None
    my_data_engine = None
    my_table_name = ""

    # initialize the engine by specifying the table name(which may consist of many sets of 5 yrs data)
    # this constructor will fetch the table and store in data member my_full_table in json format
    # then, pass the json table to data member my_calculation_engine to create the statistic agent
    def __init__(self,table_name):
        self.db_engine = db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
        self.my_full_table = self.db_engine.get_whole_table(table_name)
        self.my_data_engine = get_sim_data_engine.get_sim_data_engine(self.my_full_table)
        self.my_table_name = table_name

    # handle the real time update of the table
    # decorator function
    def update(self):
        # update the db_engine first
        self.db_engine = db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
        # get the current full table without slicing
        current_df = self.my_data_engine.get_full_df()
        current_max_timestamp = current_df['timestamp'].max()
        unique_spec = current_df['spec'].unique()
        for spec in unique_spec:
            condition = ["timestamp",">",int(current_max_timestamp.value/1000000000)]
            new_data = self.db_engine.query_all_by_condition(self.my_table_name,spec,condition)
            to_be_appended = self.my_data_engine.my_utils.json_to_dataframe(new_data)
            current_df = pd.concat([to_be_appended,current_df],ignore_index=False)

        # sort the new dataframe
        current_df.sort_values(['timestamp'],ascending=True,inplace=True,kind='stable')
        current_df.sort_values(['spec'],ascending=True,inplace=True,kind='stable')

        # update the table_df in my_data_engine
        self.my_data_engine.table_df = current_df.reset_index(drop=True)

    # assumption: the table will always follow the format which
    # the partition key is "spec" with data format "String"
    # the sort key is "timestamp" with data format "Number"
    def delete_and_init(self):
        key_attribute = {"partition_key": {"name": "spec", "keytype": "S"}, "sort_key": {"name": "timestamp", "keytype": "N"}}
        self.db_engine.dynamodb.Table(self.my_table_name).delete()
        # the deletion process is very slow
        while True:
            try:
                self.db_engine.create_table(self.my_table_name,key_attribute)
                message = ""
            except Exception as e:
                message = str(e)
            if (message == ""):
                break

    # For the following functions
    # use of **kwargs to indicate whether user want to specify a particular set of data (5y) or not
    # accept ONE (or none) keyword: "spec"
    # for every function, if enter the wrong keyword argument, then will terminate the whole application
    # Examples

    # internal use: get the whole df without slicing
    def get_full_df(self,**kwargs):
        check_kwargs(kwargs)
        self.update()
        return self.my_data_engine.get_full_df(**kwargs)

    # Return data in pandas df
    def get_data_1d(self,date,**kwargs):
        check_kwargs(kwargs)
        self.update()
        return self.my_data_engine.get_data(date,"1d",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_data_engine.get_data(date,"1d")
        
    def get_data_1m(self,date,**kwargs):
        check_kwargs(kwargs)
        self.update()
        return self.my_data_engine.get_data(date,"1m",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_data_engine.get_data(date,"1m")

    def get_data_6m(self,date,**kwargs):
        check_kwargs(kwargs)
        self.update()
        return self.my_data_engine.get_data(date,"6m",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_data_engine.get_data(date,"6m")
    
    def get_data_1y(self,date,**kwargs):
        check_kwargs(kwargs)
        self.update()
        return self.my_data_engine.get_data(date,"1y",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_data_engine.get_data(date,"1y")

    def get_data_5y(self,date,**kwargs):
        check_kwargs(kwargs)
        self.update()
        return self.my_data_engine.get_data(date,"5y",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_data_engine.get_data(date,"5y")

    # if you want to specify a day: using the same data in the array, e.g. ["2017-10-20","2017-10-20"]
    # if you want to specify a range: using different data in the array, e.g. ["2017-10-20","2018-10-20"]
    def get_data_range(self,range,**kwargs):
        check_kwargs(kwargs)
        self.update()
        return self.my_data_engine.get_data_range(range,spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_data_engine.get_data_range(range)

    def get_last_n_row(self,n,**kwargs):
        check_kwargs(kwargs)
        self.update()
        return self.my_data_engine.get_last_n_row(n,spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_data_engine.get_last_n_row(n)

    def minute_to_daily(self,df):
        return self.my_data_engine.minute_frame_to_daily_frame(df)

    # array of dictionary
    def upload_sim_data(self):
        pass

def main():
    my_API = get_sim_data_API("backtest_rebalance_margin_wif_max_drawdown_control_0")
    print(my_API.get_data_1d("2017-10-20"))
    

if __name__ == "__main__":
    main()