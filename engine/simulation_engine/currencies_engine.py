from ib_insync import *
import datetime as dt
import pandas as pd
import os
import pathlib


class currencies_engine:

    def __init__(self, ib_instance=None):
        if ib_instance:
            self.ib_instance = ib_instance
            self.ib_instance.reqMarketDataType(marketDataType=1)  # require live data
        # self.output_filepath = str(pathlib.Path(__file__).parent.parent.parent.resolve()) + f"/his_data/one_min"
        self.output_filepath = str(
            pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + "/ticker_data/one_min"

    # get the historical currency rate within the given range
    def get_historical_currency_rate_by_range(self, base_cur, dest_cur, start_timestamp, end_timestamp):
        current_end_timestamp = end_timestamp
        ticker = f'{base_cur}{dest_cur}'
        contract = Forex(ticker)
        self.ib_instance.qualifyContracts(contract)  # qualify the contract

        while current_end_timestamp > start_timestamp:
            end_date = dt.datetime.fromtimestamp(current_end_timestamp)
            current_data = self.ib_instance.reqHistoricalData(contract, end_date, whatToShow="BID",
                                                              durationStr='2 W',
                                                              barSizeSetting='1 min', useRTH=True)
            current_end_timestamp = current_data[0].date.timestamp()
            self.ib_instance.sleep(0)
            current_data_df = util.df(current_data)  # convert into df
            current_data_df['timestamp'] = current_data_df[['date']].apply(
                lambda x: x[0].replace(tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp(), axis=1).astype(int)

            # print(current_data_df)  # only for testing
            self.write_df_to_csv(ticker, current_data_df)

    def write_df_to_csv(self, ticker, df):
        """
        algoithm:
        if file already exists:
            read the file -> old data
            delete the old file
        create a new file
        write the current data to the new file (on the top) with header
        write the old data if file already exist
        """
        file_exist = f"{ticker}.csv" in os.listdir(self.output_filepath)
        if file_exist:  # file already exist
            old_df = pd.read_csv(f"{self.output_filepath}/{ticker}.csv")
            try:
                os.remove(f"{self.output_filepath}/{ticker}.csv")
            except Exception as e:
                print(f"Some errors occur, error message: {e}")

        with open(f"{self.output_filepath}/{ticker}.csv", "a+", newline='') as f:
            df.to_csv(f, mode='a', index=False, header=True)  # write the current data with header
            if file_exist:
                old_df.to_csv(f, mode='a', index=False, header=False)  # write the old data

        print(f"[{dt.datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] Successfully appended {ticker}.csv")

    def convert_to_usd(self, df_to_convert: pd.DataFrame, column_to_convert: str,
                       conversion_df: pd.DataFrame, inverse_dir: bool):
        """
        params:
        df_to_convert: the base dataframe to be converted to USD
        column_to_convert: the column name in df_to_convert to be converted
        conversion_df: dataframe which stores the base currency to USD (or inverse; we need {inverse_dir} = true)
        inverse_dir: true if the conversion_df is from USD to destination currency.
                        False if it is from destination to USD

        output a new dataframe with column_to_convert in USD to output_filepath
        """

        for row in df_to_convert.itertuples():
            timestamp_to_convert = getattr(row, 'timestamp')
            column_to_convert_value = getattr(row, column_to_convert)
            timestamp_in_conversion = timestamp_to_convert
            conversion_row = conversion_df.loc[conversion_df['timestamp'] == timestamp_to_convert]
            if not conversion_row.shape[0]:  # there is no data has the same timestamp
                # attempt to find a closer timestamp in an earlier timeframe
                for i in range(1, 60):
                    conversion_row_minus = conversion_df.loc[conversion_df['timestamp'] == timestamp_in_conversion - i, column_to_convert]
                    if conversion_row_minus:
                        delta_minus = i
                        break
                # attempt to find a closer timestamp in a later timeframe
                for i in range(1, 60):
                    conversion_row_plus = conversion_df.loc[conversion_df['timestamp'] == timestamp_in_conversion + i, column_to_convert]
                    if conversion_row_plus:
                        delta_plus = i
                        break
                if delta_plus > delta_minus:
                    timestamp_in_conversion -= delta_minus
                else:
                    timestamp_in_conversion += delta_plus
            conversion_row = conversion_df.loc[conversion_df['timestamp'] == timestamp_in_conversion]
            rate = conversion_row['close'].values[0]
            if inverse_dir:
                print(column_to_convert_value / rate)
                df_to_convert.loc[df_to_convert['timestamp'] == timestamp_to_convert,
                                  column_to_convert] = column_to_convert_value / rate
            else:
                df_to_convert.loc[df_to_convert['timestamp'] == timestamp_to_convert,
                                  column_to_convert] = column_to_convert_value * rate

        return df_to_convert


def main():
    # ib = IB()
    # ib.connect('127.0.0.1', 7497, clientId=1)

    engine = currencies_engine()
    df_to_convert = pd.read_csv('/Users/thomasli/Documents/Rainy Drop Investment/user_id_0/backtest/backtest_rebalance_margin_wif_max_drawdown_control_0/run_data/0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_.csv')
    conversion_df = pd.read_csv('/Users/thomasli/Documents/Rainy Drop Investment/ticker_data/one_min/USDCNH.csv')
    df = engine.convert_to_usd(df_to_convert, 'NetLiquidation', conversion_df, True)
    print(df)


if __name__ == '__main__':
    main()
