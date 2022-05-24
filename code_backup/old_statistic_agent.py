from collections import ChainMap
from os import listdir
from os.path import isfile, isdir, join

import pandas as pd


def _get_cleaned_data(file):
    print(file)
    df = pd.read_csv(file)
    return df


def _cal_ytd_return(_cleaned_data):
    _ytd_return = 0
    _cleaned_data['date'] = pd.to_datetime(_cleaned_data['date'])
    last_day_dt = _cleaned_data.loc[_cleaned_data['date'] == _cleaned_data['date'].max()]
    yr_of_last_day = _cleaned_data['date'].max().year
    yr_dt = _cleaned_data.loc[_cleaned_data['date'].dt.year == yr_of_last_day]
    first_day_dt = _cleaned_data.loc[_cleaned_data['date'] == yr_dt['date'].min()]

    first_day_nlv = first_day_dt['NetLiquidation(Day End)'].max()
    last_day_nlv = last_day_dt['NetLiquidation(Day End)'].max()

    _ytd_return = last_day_nlv/first_day_nlv -1

    return _ytd_return

def _cal_since_inception_return(_cleaned_data):
    _since_inception_return = 0
    _cleaned_data['date'] = pd.to_datetime(_cleaned_data['date'])

    last_day_dt = _cleaned_data.loc[_cleaned_data['date'] == _cleaned_data['date'].max()]
    first_day_dt = _cleaned_data.loc[_cleaned_data['date'] == _cleaned_data['date'].min()]
    _target_last_day_nlv = last_day_dt['NetLiquidation(Day End)'].max()
    _target_first_day_nlv = first_day_dt['NetLiquidation(Day End)'].max()

    _since_inception_return = _target_last_day_nlv/_target_first_day_nlv -1

    return _since_inception_return


def _cal_n_yr_return(_cleaned_data, n):
    n_yr_return = 0
    _cleaned_data['date'] = pd.to_datetime(_cleaned_data['date'])
    #get yr-1
    yr_of_last_day = _cleaned_data['date'].max().year
    _target_yr = yr_of_last_day - n
    _last_yr = yr_of_last_day - 1
    # get first& last day dataframe
    _target_yr_dt = _cleaned_data.loc[_cleaned_data['date'].dt.year == _target_yr]
    _last_yr_dt = _cleaned_data.loc[_cleaned_data['date'].dt.year == _last_yr]

    if not _target_yr_dt.empty and not _last_yr_dt.empty:
        print("not _target_yr_dt.empty and not _last_yr_dt.empty")
        _target_first_day_dt = _cleaned_data.loc[_cleaned_data['date'] == _target_yr_dt['date'].min()]
        _last_yr_last_day_dt = _cleaned_data.loc[_cleaned_data['date'] == _last_yr_dt['date'].max()]
        # get first& last day data
        _target_first_day_nlv = _target_first_day_dt['NetLiquidation(Day End)'].max()
        _last_yr_last_day_nlv = _last_yr_last_day_dt['NetLiquidation(Day End)'].max()

        n_yr_return = _last_yr_last_day_nlv/_target_first_day_nlv -1

    return n_yr_return


def _cal_sharpe(_target_data):
    risk_free_rate = 0
    nlv = _target_data['NetLiquidation(Day End)']
    daily_return = nlv.pct_change()
    daily_return = daily_return.dropna()

    mean_daily_return = sum(daily_return) / len(daily_return)
    # Calculate Standard Deviation
    n = len(daily_return)
    # Calculate mean
    mean = sum(daily_return) / n
    # Calculate deviations from the mean
    deviations = sum([(x - mean)**2 for x in daily_return])
    # Calculate Variance & Standard Deviation
    variance = deviations / (n - 1)
    s = variance**(1/2)

    # Calculate Daily Sharpe Ratio
    daily_sharpe_ratio = (mean_daily_return - risk_free_rate) / s
    # Annualize Daily Sharpe Ratio
    sharpe_ratio = 252**(1/2) * daily_sharpe_ratio

    return sharpe_ratio


def _cal_since_inception_sharpe(_cleaned_data):
    _target_data = _cleaned_data
    sharpe_ratio = _cal_sharpe(_target_data)
    return sharpe_ratio

def _cal_ytd_sharpe(_cleaned_data):
    _cleaned_data['date'] = pd.to_datetime(_cleaned_data['date'])
    yr_of_last_day = _cleaned_data['date'].max().year
    yr_dt = _cleaned_data.loc[_cleaned_data['date'].dt.year == yr_of_last_day]

    _target_data = yr_dt
    sharpe_ratio = _cal_sharpe(_target_data)
    return sharpe_ratio

def _cal_n_yr_sharpe(_cleaned_data, n):
    _cleaned_data['date'] = pd.to_datetime(_cleaned_data['date'])

    yr_of_last_day = _cleaned_data['date'].max().year
    last_yr = yr_of_last_day-1
    _target_yr = yr_of_last_day-n
    # get first& last day dataframe
    #_target_yr_dt = _cleaned_data.loc[_cleaned_data['date'].dt.year == _target_yr]

    _target_yr_dt = _cleaned_data.loc[(_cleaned_data['date'].dt.year >= _target_yr) & (_cleaned_data['date'].dt.year <= last_yr)]
    _target_data = _cleaned_data
    sharpe_ratio = _cal_sharpe(_target_data)

    return sharpe_ratio


def _get_start_end_year(_cleaned_data):
    _cleaned_data['date'] = pd.to_datetime(_cleaned_data['date'])
    start_year = _cleaned_data['date'].min().year
    end_year = _cleaned_data['date'].max().year
    return start_year, end_year


def _cal_max_drawdown(_cleaned_data):
    # We are going to use a trailing 252 trading day window
    window = 252
    # Calculate the max drawdown in the past window days for each day in the series.
    # Use min_periods=1 if you want to let the first 252 days data have an expanding window
    Roll_Max = _cleaned_data['NetLiquidation(Day End)'].rolling(window, min_periods=1).max()
    Daily_Drawdown = _cleaned_data['NetLiquidation(Day End)'] / Roll_Max - 1.0

    # Next we calculate the minimum (negative) daily drawdown in that window.
    # Again, use min_periods=1 if you want to allow the expanding window
    Max_Daily_Drawdown = Daily_Drawdown.rolling(window, min_periods=1).min()

    return Max_Daily_Drawdown


class statistic_agent(object):

    db_path = ""
    _sim_data_path = ""
    def __init__(self, path):
        self.db_path = path + "/db"
        self._sim_data_path = path + "/sim_data/csv"

    def cal_parameter_start_year_return_stats(self):
        _all_file_return = pd.read_csv(self.db_path + "/file_return/csv/all_file_return.csv")
        _rebalance_margin_array = []
        data_list_col ={'Chosen Value', "Best Start Yr (5 Yr Return)", "Best 5 Yr Return", "Worst Start Yr (5 Yr Return)", "Worst 5 Yr Return", "Median Start Yr (5 Yr Return)", "Median 5 Yr Return"}
        data_list = pd.DataFrame(columns=data_list_col)

        for file in listdir(self._sim_data_path):
            chosen_value = file.split("__")[0]
            if not chosen_value in _rebalance_margin_array:
                _rebalance_margin_array.append(chosen_value)
        i=0
        for value in _rebalance_margin_array:

            _return_dt = _all_file_return[_all_file_return['File Name'].str.contains(value)]
            _value_median_5_yr_return = 0
            if(len(_return_dt["5 Yr Return"])% 2 == 0):
                _value_median_5_yr_return = _return_dt["5 Yr Return"].median()
                _diff = [{abs(_value_median_5_yr_return-i):i} for i in _return_dt["5 Yr Return"]]
                _diff = dict(ChainMap(*_diff))
                _diff_key = [abs(_value_median_5_yr_return - i) for i in _return_dt["5 Yr Return"]]
                _min_diff_key = min(_diff_key)

                _value_median_5_yr_return = _diff.get(_min_diff_key)

            else:
                _value_median_5_yr_return = _return_dt["5 Yr Return"].median()


            _value_max_5_yr_return = _return_dt["5 Yr Return"].max()
            _value_min_5_yr_return = _return_dt["5 Yr Return"].min()

            _median_start_year = _return_dt.loc[_return_dt["5 Yr Return"] == _value_median_5_yr_return].get("Start Year").values[0]
            _max_start_year = _return_dt.loc[_return_dt["5 Yr Return"] == _value_max_5_yr_return].get("Start Year").values[0]
            _min_start_year = _return_dt.loc[_return_dt["5 Yr Return"] == _value_min_5_yr_return].get("Start Year").values[0]

            data = {'Chosen Value':value, "Best Start Yr (5 Yr Return)":_max_start_year, "Best 5 Yr Return":_value_max_5_yr_return,
                    "Worst Start Yr (5 Yr Return)":_min_start_year, "Worst 5 Yr Return":_value_min_5_yr_return,
                    "Median Start Yr (5 Yr Return)":_median_start_year, "Median 5 Yr Return":_value_median_5_yr_return}
            data_list = data_list.append(data, ignore_index=True)
            data_list = data_list[['Chosen Value', "Best Start Yr (5 Yr Return)", "Best 5 Yr Return",
                    "Worst Start Yr (5 Yr Return)", "Worst 5 Yr Return",
                    "Median Start Yr (5 Yr Return)", "Median 5 Yr Return"]]
            i+=1
        data_list.to_csv(self.db_path + "/file_return/csv/rebalance_margin_return_stats.csv")
        data_list.to_json(self.db_path + "/file_return/db/rebalance_margin_return_stats.json", orient="records")

        pass



    def cal_file_return(self, file):

        file_path = self._sim_data_path + "/" + file
        _cleaned_data = _get_cleaned_data(file_path)
        _start_year, _end_year = _get_start_end_year(_cleaned_data)

        since_inception_return = _cal_since_inception_return(_cleaned_data)
        _ytd_return = _cal_ytd_return(_cleaned_data)
        _1_yr_return = _cal_n_yr_return(_cleaned_data, 1)
        _3_yr_return = _cal_n_yr_return(_cleaned_data, 3)
        _5_yr_return = _cal_n_yr_return(_cleaned_data, 5)

        _since_inception_sharpe = _cal_since_inception_sharpe(_cleaned_data)
        _ytd_sharpe = _cal_ytd_sharpe(_cleaned_data)
        _1_yr_sharpe = _cal_n_yr_sharpe(_cleaned_data, 1)
        _3_yr_sharpe = _cal_n_yr_sharpe(_cleaned_data, 3)
        _5_yr_sharpe = _cal_n_yr_sharpe(_cleaned_data, 5)

        # _max_drawdown = _cal_max_drawdown(_cleaned_data)

        _data = {'File Name':file, "Start Year":_start_year, "End Year":_end_year,'YTD Return':_ytd_return, '1 Yr Return':_1_yr_return, "3 Yr Return": _3_yr_return, "5 Yr Return": _5_yr_return, "Since Inception":since_inception_return, "Since Inception Sharpe":_since_inception_sharpe, "YTD Sharpe":_ytd_sharpe, "1 Yr Sharpe":_1_yr_sharpe, "3 Yr Sharpe":_3_yr_sharpe, "5 Yr Sharpe":_5_yr_sharpe}

        return _data




    def _cal_return(self, start_date, end_date, file):
        start_date_nlv = file.loc[start_date].at['NetLiquidation(Day End)']
        end_date_nlv = file.loc[end_date].at['NetLiquidation(Day End)']
        _return = end_date_nlv/ start_date_nlv

        return _return

    def cal_month_to_month_breakdown(self, file):
        file_path = self._sim_data_path + "/" + file
        _cleaned_data = _get_cleaned_data(file_path)
        _start_year, _end_year = _get_start_end_year(_cleaned_data)

        start_year = _cleaned_data['date'].min().year
        end_year = _cleaned_data['date'].max().year
        for year in range(start_year, end_year + 1):
            data_list = []
            yr_dt = _cleaned_data.loc[_cleaned_data['date'].dt.year == year]
            start_month = yr_dt['date'].min().month
            end_month = yr_dt['date'].max().month
            # get yr dataframe

            for month in range(start_month, end_month + 1):
                month_dt = yr_dt.loc[yr_dt['date'].dt.month == month]
                month_start_date_dt = month_dt.loc[month_dt['date'] == month_dt['date'].min()]
                month_start_date_nlv = month_start_date_dt['NetLiquidation(Day End)'].max()

                if month <= 11:
                    month_next_month_dt = yr_dt.loc[yr_dt['date'].dt.month == (month + 1)]
                else:
                    yr_dt = _cleaned_data.loc[_cleaned_data['date'].dt.year == (year + 1)]
                    month_next_month_dt = yr_dt.loc[yr_dt['date'].dt.month == 1]

                month_next_month_start_date_dt = month_next_month_dt.loc[
                    month_next_month_dt['date'] == month_next_month_dt['date'].min()]
                month_next_month_start_date_nlv = month_next_month_start_date_dt['NetLiquidation(Day End)'].max()
                monthly_return = (month_next_month_start_date_nlv - month_start_date_nlv) / month_start_date_nlv

                list = {"Year": year, "Month": month, "Monthly Return": monthly_return,
                        "Month Start Date NLV": month_start_date_nlv,
                        "Month End Date NLV": month_next_month_start_date_nlv}
                data_list.append(list)

        df = pd.DataFrame(data_list)
        df.fillna(0)
        df = df[["Year", "Month", "Monthly Return", "Month Start Date NLV", "Month End Date NLV"]]

        _month_to_month_breakdown_excel_name = self.db_path + "/file_return/csv/m2m_" + file
        df.to_csv(_month_to_month_breakdown_excel_name)
        df.to_json(self.db_path + "/file_return/db/m2m_" + file.split(".csv")[0]+".json", orient="records")
        pass

