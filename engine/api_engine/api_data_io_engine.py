import pandas as pd
from IndexTradingApplication.engine.mongoDB_engine import mongodb_engine
from IndexTradingApplication.engine.simulation_engine import statistic_engine


class api_data_io_engine:
    timestamp = []
    input_df = ""

    def __init__(self, _input_df, _timestamp):
        self.input_df = _input_df
        self.timestamp = _timestamp

    def get_netliquidation_json(self):
        temp_df = self.input_df[['timestamp', 'NetLiquidation']].copy()
        output_json = self.convert_df_to_json(temp_df)
        return output_json

    def get_buy_sell_info_json(self):
        temp_df = self.input_df[['timestamp', 'buy/sell', 'quantity', 'price', 'totalAmount']].copy()
        output_json = self.convert_df_to_json(temp_df)
        return output_json

    def get_drawdown_json(self):

        return drawdown_json

    def get_mulitple_info_dict(self):
        output_dict ={}
        #total return %, net profit, net liquidation, sharpe ratio, compounding return, margin ratio
        #code below
        return output_dict

    def convert_df_to_json(self, df):
        json_file = df.to_json(orient='records')
        return json_file


def main():
    return

if __name__ == "__main__":
    main()