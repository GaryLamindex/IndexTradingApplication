import os
import sys
import pandas as pd

# grap the dynamo_db_engine class
script_dir = os.path.dirname(__file__)
db_engine_dir = os.path.join(script_dir, '..', 'aws_engine')
sys.path.append(db_engine_dir)

import dynamo_db_engine as db_engine

# grab the statistic_agent class
script_dir = os.path.dirname(__file__)
simulation_engine_dir = os.path.join(script_dir, '..', 'simulation_engine')
sys.path.append(simulation_engine_dir)

import new_statistic_agent as stat_agent

# global helper function to check the validity of kwargs
# the only possible kwargs in the class functions below will be "spec"
def check_kwargs(kwargs):
    if (len(kwargs) > 1 or len(kwargs) == 1 and list(kwargs.keys())[0] != "spec"):
        sys.exit("Wrong parameter")

class data_engine:
    # private data members # modify later
    db_engine = None
    my_full_table = None
    my_calculation_engine = None

    # initialize the engine by specifying the table name(which may consist of many sets of 5 yrs data)
    # this constructor will fetch the table and store in data member my_full_table in json format
    # then, pass the json table to data member my_calculation_engine to create the statistic agent
    def __init__(self,table_name):
        self.db_engine = db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
        self.my_full_table = self.db_engine.get_whole_table(table_name)
        self.my_calculation_engine = stat_agent.statistic_agent(self.my_full_table)

    # For the following functions
    # use of **kwargs to indicate whether user want to specify a particular set of data (5y) or not
    # accept ONE (or none) keyword: "spec"
    # for every function, if enter the wrong keyword argument, then will terminate the whole application
    # Examples

    # 1. Return data in Json format
    def get_data_1d(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_data(date,"1d",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_data(date,"1d")
        
    def get_data_1m(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_data(date,"1m",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_data(date,"1m")

    def get_data_6m(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_data(date,"6m",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_data(date,"6m")
    
    def get_data_1y(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_data(date,"1y",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_data(date,"1y")

    def get_data_5y(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_data(date,"5y",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_data(date,"5y")

    # if you want to specify a day: using the same data in the array, e.g. ["2017-10-20","2017-10-20"]
    # if you want to specify a range: using different data in the array, e.g. ["2017-10-20","2018-10-20"]
    def get_data_range(self,range,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_data_range(range,spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_data_range(range)

    # 2. Return the investment return
    def get_return_1d(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_return(date,"1d",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_return(date,"1d")

    def get_return_1m(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_return(date,"1m",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_return(date,"1m")

    def get_return_6m(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_return(date,"6m",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_return(date,"6m")

    def get_return_1y(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_return(date,"1y",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_return(date,"1y")

    def get_return_5y(self,date,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_return(date,"5y",spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_return(date,"5y")

    def get_return_inception(self,**kwargs):
        pass

    def get_return_range(self,range,**kwargs):
        check_kwargs(kwargs)
        return self.my_calculation_engine.get_return_range(range,spec=list(kwargs.values())[0]) if len(kwargs) == 1 else self.my_calculation_engine.get_retun_range(range)

    # 3. Return the Sharpe ratio


def main():
    my_engine = data_engine("backtest_rebalance_margin_wif_max_drawdown_control_0")
    # df = my_engine.my_calculation_engine.table_df
    # print(df)
    # df.to_pickle("testing.txt")
    # print(my_engine.my_calculation_engine.table_df)
    while(True):
        date1 = input("input start date: ")
        date2 = input("input end date: ")
        if date1 == "" or date2 == "": 
            break

        range = [date1, date2]

        # date = input("input a date: ")
        if date1 == "" or date2 == "":
            break

        result = my_engine.get_return_range(range,spec="0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0")

        print(result)
    

if __name__ == "__main__":
    main()