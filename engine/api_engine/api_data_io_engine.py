import pandas as pd


class api_data_io_engine:
    timestamp = []

    def __init__(self, _timestamp):
        self.timestamp = _timestamp

    def get_netliquidation_df(self, input_df):
        output_df = pd.DataFrame(columns=['timestamp','NetLiquidation'])
        #Code below

        return output_df

    def get_buy_sell_info_df(self, input_df):
        buysell_df = pd.DataFrame(columns=['timestamp', 'buy/sell', 'quantity', 'price', 'totalAmount'])
        # Code below

        return buysell_df

    def get_mulitple_info_dict(self):
        output_dict ={}
        #total return %, net profit, net liquidation, sharpe ratio, compounding return, margin ratio
        #code below
        return output_dict

    def convert_df_to_json(self, input_df):
        #code below
        return json_file


def main():

if __name__ == main():
    main()