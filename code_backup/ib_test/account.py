from ib_insync import *
import pathlib
import os
import datetime as dt
import pandas as pd
from time import sleep

class account_info:
    path = ""
    ib_instance = None

    def __init__(self,ib_instance):
        self.path = pathlib.Path(__file__).resolve().parent
        self.ib_instance = ib_instance

    # write one row according to the snpashot of the account
    def update_account_db(self):
        # fetch the account snapshot
        raw_balance = self.ib_instance.accountSummary('All')
        # create a dict to be written to the csv, and also a list of interested items 
        # the values of the dictionary should be in array / list form
        item_dict = {'Date':[dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')],'Timestamp':[dt.datetime.now().timestamp()]}
        item_list = ['TotalCashBalance','StockMarketValue','NetLiquidationByCurrency','UnrealizedPnL','RealizedPnL','ExchangeRate']
        for i in range(len(raw_balance)):
            if (raw_balance[i][3] == 'USD' and raw_balance[i][1] in item_list):
                item_dict[raw_balance[i][1]] = [raw_balance[i][2]]
        # handle the positions
        for position in self.ib_instance.portfolio():
            symbol = position[0].symbol
            position_item_list = [f'{symbol} position', f'{symbol} marketPrice', f'{symbol} marketValue', f'{symbol} averageCost', f'{symbol} unrealizedPNL', f'{symbol} realizedPNL']
            i = 1 # to help keep track of the position of information in each PortfolioItem
            for position_item in position_item_list:
                item_dict[position_item] = position[i]
                i += 1
        # create a dataframe for the dictionary
        item_df = pd.DataFrame.from_dict(item_dict,orient='columns')
        # write the dataframe to a csv named "account_db.csv", two possible cases: the file already exists, or the file does not exist
        if ('account_db.csv' in os.listdir(self.path)): # already exist
            with open('account_db.csv','a',newline='') as f:
                item_df.to_csv(f,mode='a',index=False,header=False)
        else: # not exist
            with open('account_db.csv','a',newline='') as f:
                item_df.to_csv(f,mode='a',index=False,header=True)

    # the parameter timeframe is the time-lapse bewteen 2 function calls in second
    def continuously_update_account_db(self,timeframe):
        while True:
            self.update_account_db()
            sleep(timeframe)

def main():
    ib = IB()
    ib.connect('127.0.0.1',7497,clientId=1)
    # raw_balance = ib.accountSummary('All')
    # item_list = ['TotalCashBalance','StockMarketValue','NetLiquidationByCurrency','UnrealizedPnL','RealizedPnL','ExchangeRate']
    # for i in range(len(raw_balance)):
    #     if (raw_balance[i][3] == 'USD' and raw_balance[i][1] in item_list):
    #         print(raw_balance[i][2])
    # account = account_info(ib)
    # account.update_account_db()
    account = account_info(ib)
    account.continuously_update_account_db(30)
    # positions = ib.portfolio()
    # for position in positions:
    #     print(position[0].symbol)
    #     for i in range(1,6):
    #         print(position[i])

if __name__ == "__main__":
    main()