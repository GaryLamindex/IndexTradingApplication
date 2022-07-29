import os
import pathlib
import sys
import pandas as pd
import json
import datetime as dt
from decimal import Decimal
from ib_insync import *
from time import sleep
import numpy as np

# import pythonProject.engine.aws_engine.dynamo_db_engine as db_engine

# grab the dynamo_db_engine class
from engine.backtest_engine.grab_yfinance_data import yfinance_data_engine

script_dir = os.path.dirname(__file__)
db_engine_dir = os.path.join(script_dir, '..', 'aws_engine')
sys.path.append(db_engine_dir)

stock_data_engine_dir = os.path.join(script_dir, '..', 'realtime_engine_ibkr')
sys.path.append(stock_data_engine_dir)


# for handle decimal serialization in JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def query_result_to_dataframe(result):
    data_string = json.dumps(result, cls=DecimalEncoder)
    df = pd.read_json(data_string, orient='records')
    return df


def is_leap_year(year):
    if ((year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)):
        return True
    else:
        return False


# return a list in the format [<year>,<month>,<day>]
def go_previous_day(day, month, year):
    day_30_months = [4, 6, 9, 11]
    day_31_months = [1, 3, 5, 7, 8, 10, 12]

    if day == 1:
        if month == 1:
            return [year - 1, 12, 31]
        elif month == 3:  #
            if is_leap_year(year):
                return [year, 2, 29]
            else:
                return [year, 2, 28]
        elif (month - 1 + 12) % 12 == 0 or (month - 1 + 12) % 12 in day_31_months:
            new_month = 12 if (month - 1 + 12) % 12 == 0 else (month - 1 + 12) % 12
            return [year, new_month, 31]
        elif (month - 1 + 12) % 12 in day_30_months:
            return [year, (month - 1 + 12) % 12, 30]
    else:
        return [year, month, day - 1]


# not in use, NOT yet modified.
class online_engine:
    # default table name: "ticket_data"
    db_engine = None
    freq = ""

    def __init__(self, tickers, freq):  # tickers in this case is a dummy, since will not load the data in advance
        self.db_engine = self.db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
        self.freq = freq

    def get_n_days_data(self, ticker, timestamp, n):  # the timestamp parameter MUST be an integer or a float
        """
        Example: get_n_days_data("QQQ",1630419240,5), timestamp 1630419240 stands for "2021-08-31 14:14:00"
        The above funciton will thus return a dataframe of data of 5 trading days BEFORE 2021-08-31
        """
        dt_obj = dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc)
        year = dt_obj.year
        month = dt_obj.month
        day = dt_obj.day

        # need to acquire n days data, but it is possible to retrieve nothing for a certain day (e.g. holiday...)
        # update the counter if the queried data is NOT empty
        count = 0

        df_list = []

        while (count < n):
            # update the date info to the previous day
            prev_date_list = go_previous_day(day, month, year)
            year = prev_date_list[0]
            month = prev_date_list[1]
            day = prev_date_list[2]

            # trading range inside a day: 13:30:00-20:59:59 (take care of both the summer and the winter period)
            lower_datestring = f'{year}-{month}-{day} 13:30:00'
            upper_datestring = f'{year}-{month}-{day} 20:59:59'

            lower_dt = dt.datetime.strptime(lower_datestring, "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
            upper_dt = dt.datetime.strptime(upper_datestring, "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)

            query_range = [int(lower_dt.timestamp()), int(upper_dt.timestamp())]

            query_result = self.db_engine.query_all_by_range(f"his_data_{self.freq}", ticker, "timestamp", query_range)

            if (len(query_result) != 0):
                df_list.append(query_result_to_dataframe(query_result))
                # update count
                count += 1

        # process and return the final dataframe
        n_days_data_df = pd.concat(df_list)

        n_days_data_df.sort_values(['timestamp'], ascending=True, kind='stable', inplace=True)

        n_days_data_df.reset_index(drop=True, inplace=True)

        return n_days_data_df

    def get_data_by_range(self, ticker, range):
        """
        Example: 
        range = [1527082200,1527082740]
        get_data_by_range("QQQ",range) will return a dataframe within the specified range
        """

        query_result = self.db_engine.query_all_by_range(f"his_data_{self.freq}", ticker, "timestamp", range)

        data_by_range_df = query_result_to_dataframe(query_result)

        data_by_range_df.sort_values(['timestamp'], ascending=True, kind='stable', inplace=True)

        data_by_range_df.reset_index(drop=True, inplace=True)

        return data_by_range_df

    def get_ticker_item_by_timestamp(self, ticker, timestamp):
        """
        return a single row of data with the exact match with the specified timestamp
        Example: get_data_by_range("QQQ",1508429100) will return a dictionary of the record in QQQ data with the same timestamp as 1508429100
        """

        query_result = self.db_engine.query_all_by_range(f"his_data_{self.freq}", ticker, "timestamp",
                                                         [timestamp, timestamp])

        ticker_row = query_result_to_dataframe(query_result).reset_index(drop=True)

        ticker_dict = ticker_row.to_dict(orient='index')[0]

        return ticker_dict


# each engine object suppports only one ticker
class local_engine:
    full_ticker_df = None  # a dictionary of data df object (consist of tickers items)
    filepath = ""
    ticker = ""

    def __init__(self, ticker, freq):
        # load all data from /his_data all inside the engine first
        # example of format of "freq": "one_min"
        # self.filepath = str(pathlib.Path(__file__).parent.parent.parent.resolve()) + f"/his_data/{freq}"
        self.filepath = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/ticker_data/{freq}"
        self.ticker = ticker
        """to be modified"""
        # if freq == "one_day":
        #     engine = yfinance_data_engine()
        #     df = engine.get_yfinance_max_historical_data(ticker)
        #     index_list = df.index.tolist()
        #     timestamp = list()
        #     for x in range(len(index_list)):
        #         timestamp.append(int(index_list[x].timestamp()))
        #     df['timestamp'] = timestamp
        #     df = df.rename(columns={'Open': 'open'})
        #     df.to_csv(f"{engine.ticker_data_path}/{ticker}.csv", index=True, header=True)
        #     print(f"Successfully download {ticker}.csv")
        self.full_ticker_df = pd.read_csv(f"{self.filepath}/{ticker}.csv")
        # print("Fetch data from filepath:", self.filepath)
        # for ticker in tickers:
        #     # loads the file one by one
        #     with open(f'{self.filepath}/{ticker}.json') as json_file:
        #         ticker_data_json = json.load(json_file)
        #         ticker_data_dict = ticker_data_json['_default']
        #     self.full_data_df_list[ticker] = pd.DataFrame.from_dict(ticker_data_dict,orient='index').reset_index(drop=True)
        #     self.full_data_df_list[ticker]['timestamp'] = self.full_data_df_list[ticker]['timestamp'].astype(float)
        # print("His data successfully loaded")
        # print(self.full_data_df_list)
        a=0
    # use US timezone for comparison
    def get_n_days_data(self, timestamp, n):  # the timestamp parameter MUST be an integer or a float
        """
        Example: get_n_days_data("QQQ",1630419240,5)
        """
        dt_obj = dt.datetime.fromtimestamp(timestamp, tz=dt.timezone(dt.timedelta(hours=-5)))
        year = dt_obj.year
        month = dt_obj.month
        day = dt_obj.day

        # need to acquire n days data, but it is possible to retrieve nothing for a certain day (e.g. holiday...)
        # update the counter if the queried data is NOT empty
        count = 0

        df_list = []

        while count < n:
            # update the date info to the previous day
            prev_date_list = go_previous_day(day, month, year)
            year = prev_date_list[0]
            month = prev_date_list[1]
            day = prev_date_list[2]

            # trading range
            lower_datestring = f'{year}-{month}-{day} 00:00:01'
            upper_datestring = f'{year}-{month}-{day} 23:59:59'

            lower_dt = dt.datetime.strptime(lower_datestring, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=dt.timezone(dt.timedelta(hours=-5)))
            upper_dt = dt.datetime.strptime(upper_datestring, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=dt.timezone(dt.timedelta(hours=-5)))

            selection_range = [float(lower_dt.timestamp()), float(upper_dt.timestamp())]

            temp_df = self.full_ticker_df[(self.full_ticker_df['timestamp'] >= selection_range[0]) & (
                        self.full_ticker_df['timestamp'] <= selection_range[1])]

            if temp_df.shape[0] != 0:
                df_list.append(temp_df)
                count += 1

        # process and return the final dataframe
        n_days_data_df = pd.concat(df_list)

        n_days_data_df.sort_values(['timestamp'], ascending=True, kind='stable', inplace=True)

        n_days_data_df.reset_index(drop=True, inplace=True)

        return n_days_data_df

    def get_data_by_range(self, range):
        """
        Example: 
        range = [1527082200,1527082740]
        get_data_by_range("QQQ",range) will return a dataframe within the specified range
        """
        #print("get_data_by_range:", self.ticker, "; ", range)
        data_by_range_df = self.full_ticker_df[(self.full_ticker_df['timestamp'] >= float(range[0])) & (
                    self.full_ticker_df['timestamp'] <= float(range[1]))].copy()
        data_by_range_df.sort_values(['timestamp'], ascending=True, kind='stable', inplace=True)
        data_by_range_df.reset_index(drop=True, inplace=True)
        if data_by_range_df.size > 0:
            print("data_by_range_df")
            #/print(data_by_range_df)
            return data_by_range_df
        else:
            return None

    def get_ticker_item_by_timestamp(self, timestamp):
        """
        return a single row of data with the exact match with the specified timestamp
        Example: get_data_by_range("QQQ",1508429100) will return a dictionary of the record in QQQ data with the same timestamp as 1508429100
        """

        ticker_df = self.full_ticker_df

        ticker_row = ticker_df[ticker_df['timestamp'] == float(timestamp)].reset_index(drop=True)

        if not ticker_row.empty:
            ticker_dict = ticker_row.to_dict(orient='index')[0]
            return ticker_dict
        else:
            return None

    # not in use
    # DON'T CALL THIS FUNCTION
    def write_ticker_info(self, ticker, ticker_df):
        # os.remove(f"{self.filepath}/{ticker}.csv")

        if (f"{self.filepath}/{ticker}.csv" in os.listdir(self.filepath)):  # already exists
            with open(f"{self.filepath}/{ticker}.csv", "a+", newline='') as f:
                ticker_df.to_csv(f, mode='a', index=False, header=False)
        else:  # not exist
            with open(f"{self.filepath}/{ticker}.csv", "a+", newline='') as f:
                ticker_df.to_csv(f, mode='a', index=False, header=True)

    # def append_ticker_info(self,ticker_dict): # to an existing file
    #     for key in list(ticker_dict.keys()):
    #         with open(f"{self.filepath}/{key}.json","a+") as f:
    #             existing_list = json.load(f)
    #             new_list = existing_list + ticker_dict[key]
    #             f.seek(0)
    #             f.write(new_list)
    #             f.truncate()

    # series_1 and series_2 are columns sliced form a dataframe
    def get_union_timestamps(self, series_1, series_2):
        # return an np array
        np_arr_1 = series_1.to_numpy()
        np_arr_2 = series_2.to_numpy()
        return np.union1d(np_arr_1, np_arr_2)

    def get_intersect_timestamps(self, series_1, series_2):
        # return an np array
        np_arr_1 = series_1.to_numpy()
        np_arr_2 = series_2.to_numpy()
        return np.intersect1d(np_arr_1, np_arr_2)

def main():
    # print(my_engine.get_n_days_data("QQQ",1630419240,5))
    # print(my_engine.get_n_days_data("SPY",1630419240,5))
    # print(my_engine.get_data_by_range("QQQ",[1508428800,1508429340]))
    # print(my_engine.get_ticker_item_by_timestamp("SPY",1527082200))
    # # while True:
    #     timestamp1 = input("Enter a timestamp: ")
    #     timestamp2 = input("Enter a timestamp: ")
    #     if (timestamp1 == "" or timestamp2 == ""):
    #         break
    #     df = my_engine.get_data_by_range("QQQ",(float(timestamp1),float(timestamp2)))
    #     print(df)
    #     print(df.index[1])
    # while True:
    #     timestamp = input("Enter a timestamp: ")
    #     print(my_engine.get_ticker_item_by_timestamp("SPY",timestamp))

    # new_list = existing_list + dict_list[key]
    # f.seek(0)
    # f.write(new_list)
    # f.truncate()
    # my_engine.write_ticker_info(dict_list)
    # io_engine = local_engine("QQQ","")
    # print(io_engine.get_n_days_data(1630419240,5))
    # print(io_engine.get_ticker_item_by_timestamp(1630367940))
    # print(io_engine.get_data_by_range([1629792000,1630367940]))

    io_engine_1 = local_engine("SPY", "one_min")
    # io_engine_2 = local_engine("QQQ","one_min")
    # # print(io_engine.get_n_days_data(1630419240,5))
    # # print(io_engine.get_ticker_item_by_timestamp(1630367940))
    # series_1 = io_engine_1.get_data_by_range([1609430400,1643594952])['timestamp']
    # series_2 = io_engine_2.get_data_by_range([1609430400,1643594952])['timestamp']
    # print(series_1.shape[0])
    # print(series_2.shape[0])
    # print(io_engine_1.get_union_timestamps(series_1,series_2))
    # print(len(io_engine_1.get_union_timestamps(series_1,series_2)))
    print(io_engine_1.get_ticker_item_by_timestamp(1))


if __name__ == "__main__":
    main()
