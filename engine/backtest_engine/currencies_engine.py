import numpy
from ib_insync import *
import datetime as dt
import pandas as pd
import os
import pathlib


class currencies_engine:

    def __init__(self, ib_instance=None, run_data_path=None):
        if ib_instance:
            self.ib_instance = ib_instance
            self.ib_instance.reqMarketDataType(marketDataType=1)  # require live data
        # self.output_filepath = str(pathlib.Path(__file__).parent.parent.parent.resolve()) + f"/his_data/one_min"
        self.ticker_data_path = str(
            pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + "/ticker_data/one_min"

        if run_data_path:
            self.run_data_path = run_data_path

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
        file_exist = f"{ticker}.csv" in os.listdir(self.ticker_data_path)
        if file_exist:  # file already exist
            old_df = pd.read_csv(f"{self.ticker_data_path}/{ticker}.csv")
            try:
                os.remove(f"{self.ticker_data_path}/{ticker}.csv")
            except Exception as e:
                print(f"Some errors occur, error message: {e}")

        with open(f"{self.ticker_data_path}/{ticker}.csv", "a+", newline='') as f:
            df.to_csv(f, mode='a', index=False, header=True)  # write the current data with header
            if file_exist:
                old_df.to_csv(f, mode='a', index=False, header=False)  # write the old data

        print(f"[{dt.datetime.now().strftime('%Y/%m/%d %H:%M:%S')}] Successfully appended {ticker}.csv")

    def get_the_closet_exchange_rate(self, conversion_df, timestamp):
        conversion_row = conversion_df.loc[conversion_df['timestamp'] == timestamp, 'close']
        if not conversion_row.shape[0]:
            delta_plus, delta_minus = numpy.nan, numpy.nan
            # attempt to find a closer timestamp in an earlier timeframe
            for i in range(1, 60):
                conversion_row_minus = conversion_df.loc[conversion_df['timestamp'] == timestamp - i, 'close']
                if not conversion_row_minus.empty:
                    delta_minus = i
                    break

            for i in range(1, 60):
                conversion_row_plus = conversion_df.loc[conversion_df['timestamp'] == timestamp + i, 'close']
                if not conversion_row_plus.empty:
                    delta_plus = i
                    break

            if delta_plus > delta_minus:
                timestamp -= delta_minus
            else:
                timestamp += delta_plus
        return conversion_df.loc[conversion_df['timestamp'] == timestamp, 'close'].array[0]

    def get_conversion_df(self, base_cur, dest_cur):
        return pd.read_csv(f'{self.ticker_data_path}/{base_cur}{dest_cur}.csv')

    def net_liq_to_usd(self, df_to_convert, conversion, output_filename):
        assert len(conversion) == 6
        first_cur = conversion[:3]
        second_cur = conversion[3:6]

        try:
            conversion_df = self.get_conversion_df(first_cur, second_cur)
            invert_dir = False
        except FileNotFoundError:
            try:
                conversion_df = self.get_conversion_df(second_cur, first_cur)
                invert_dir = True
            except FileNotFoundError:
                raise FileNotFoundError('currencies engine.net_liq_to_usd() cannot find the currencies ticker data')

        for row in df_to_convert.itertuples():
            timestamp = row.timestamp
            rate = self.get_the_closet_exchange_rate(conversion_df, timestamp)
            df_to_convert.loc[df_to_convert['timestamp'] == timestamp, 'NetLiquidation'] = row.NetLiquidation / rate \
                if invert_dir else row.NetLiquidation * rate

        df_to_convert.to_csv(f'{self.run_data_path}/{output_filename}')

        return df_to_convert


def main():
    # ib = IB()
    # ib.connect('127.0.0.1', 7497, clientId=1)

    engine = currencies_engine(run_data_path='/Users/thomasli/Documents/Rainy Drop Investment/user_id_0/backtest/backtest_rebalance_margin_wif_max_drawdown_control_0/run_data')
    df_to_convert = pd.read_csv('/Users/thomasli/Documents/Rainy Drop Investment/user_id_0/backtest/backtest_rebalance_margin_wif_max_drawdown_control_0/run_data/0.06_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_.csv')
    df = engine.net_liq_to_usd(df_to_convert, 'HKDUSD', 'usd.csv')
    print(df)


if __name__ == '__main__':
    main()
