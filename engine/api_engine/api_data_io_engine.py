import pandas as pd
from engine.mongoDB_engine import mongdb_engine

class api_data_io_engine:
    timestamp = []
    input_df = ""

    def __init__(self, _input_df, _timestamp):
        self.input_df = _input_df
        self.timestamp = _timestamp

    def get_netliquidation_df(self):
        output_df = pd.DataFrame(columns=['timestamp','NetLiquidation'])
        #Code below
        output_df = self.input_df[['timestamp', 'NetLiquidation']].copy()
        return output_df

    def get_buy_sell_info_df(self):
        buysell_df = pd.DataFrame(columns=['timestamp', 'buy/sell', 'quantity', 'price', 'totalAmount'])
        # Code below


        return buysell_df

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