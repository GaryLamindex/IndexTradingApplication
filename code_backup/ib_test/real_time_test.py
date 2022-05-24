import ibkr_trade_agent
from random import random
from ib_insync import *
from time import sleep
import datetime as dt
import pandas as pd
import pathlib
import os

# generate a random integer inside the given range
# range is a list, e.g. [5,10] meaning the lower limit of the range is 5, upper limit of the range is 10 (both inclusive)
def get_random_number(range):
    return range[0] + round(((range[1] - range[0]) * random()))

# note that a trade may NOT be fully filled at a time !
def order_filled(trade,fill):
    print(trade)
    print(fill)
    # fill_dict = {"ID":[trade.order.orderId],"date":[trade.log[0].time],"ticker":[self.ticker],"market price":[price_list["market"]],"action":action_list[str(trade_signal)],"quantity":[trade_quantity],"limit price":[limit_price]}

class real_time_test_strategy:
    ib_instance = None
    trade_agent = None
    ticker = ""
    ticker_contract = None
    path = ""

    def __init__(self,ib_instance,ticker):
        self.ib_instance = ib_instance
        self.trade_agent = ibkr_trade_agent.ibkr_trade_agent(ib_instance)
        self.ticker = ticker
        self.ticker_contract = Stock(self.ticker,"SMART","USD")
        self.ib_instance.qualifyContracts(self.ticker_contract)
        self.path = pathlib.Path(__file__).resolve().parent

    def order_cancelled(self,trade):
        info_dict = {"ID":[trade.order.orderId],"date":[trade.log[1].time],"ticker":[trade.contract.symbol],"action":[trade.order.action],"quantity":[trade.order.totalQuantity],"limit price":[trade.order.lmtPrice],"message":[trade.log[1].message]}
        info_df = pd.DataFrame.from_dict(info_dict,orient="columns")
        # write the datafrmae to a csv named "trade_log.csv"
        if ("cancel_log.csv" in os.listdir(self.path)):
            with open("cancel_log.csv","a",newline='') as f:
                info_df.to_csv(f,mode='a',index=False,header=False)
        else:
            with open("cancel_log.csv","a",newline='') as f:
                info_df.to_csv(f,mode='a',index=False,header=True)

    # mannually add 1.5 for buy limit order, subtract 1.5 for sell limit order
    # for action, 1 stands for buy, 2 stands for sell
    # return a dictionary of market price and limit price
    def get_price(self,action):
        data_subscription = self.ib_instance.reqMktData(self.ticker_contract)
        self.ib_instance.sleep(2) # for the price to get filled
        market_price = data_subscription.marketPrice()
        # or use bid / ask price ?
        # possible method: reqTickers
        if action == 1: # buy
            return {"limit":round(market_price + 1.5,1),"market":market_price}
        elif action == 2: # buy
            return {"limit":round(market_price - 1.5,1),"market":market_price}
    
    def simulate_trade(self):
        # 2 possible signals (assuming always have signal): 1 stands for buy, 2 stands for sell
        trade_signal = get_random_number([1,2])
        print("trade signal:", trade_signal)
        # quantity of the stop lies between 5 and 10
        trade_quantity = get_random_number([5,10])
        price_list = self.get_price(trade_signal)
        limit_price = price_list["limit"]
        if trade_signal == 1: # buy
            trade = self.trade_agent.place_buy_stock_limit_order(self.ticker,trade_quantity,limit_price)
            self.ib_instance.sleep(2)
        elif trade_signal == 2: # sell
            trade = self.trade_agent.place_sell_stock_limit_order(self.ticker,trade_quantity,limit_price)
            self.ib_instance.sleep(2)
        # check if the trade is executed successfully (if NOT none, then is ok)
        if trade is None:
            print("Unsuccessful order")
            return
        # create handler
        def order_filled(trade,fill):
            print(trade)
            print(fill)
        # connecting events and handler
        trade.fillEvent += order_filled
        self.ib_instance.sleep(10)
        # trade.cancelEvent += order_cancelled
        # write the order details to a log
        action_list = {"1":"BUY","2":"SELL"}
        trade_details = {"ID":[trade.order.orderId],"date":[trade.log[0].time],"ticker":[self.ticker],"market price":[price_list["market"]],"action":action_list[str(trade_signal)],"quantity":[trade_quantity],"limit price":[limit_price]}
        # print the trade details to a csv file
        trade_df = pd.DataFrame.from_dict(trade_details,orient="columns")
        # write the datafrmae to a csv named "trade_log.csv"
        if ("trade_log.csv" in os.listdir(self.path)):
            with open("trade_log.csv","a",newline='') as f:
                trade_df.to_csv(f,mode='a',index=False,header=False)
        else:
            with open("trade_log.csv","a",newline='') as f:
                trade_df.to_csv(f,mode='a',index=False,header=True)
        self.ib_instance.run()
    
    def continuously_trade(self):
        pass

def main():
    ib = IB()
    ib.connect("127.0.0.1",7497,clientId=1)
    test = real_time_test_strategy(ib,"QQQ")
    # print(test.get_price(1))
    # test.simulate_trade()
    # ib.sleep(1000)
    stock = Stock("QQQ","SMART","USD")
    trader = ibkr_trade_agent.ibkr_trade_agent(ib)
    trade = trader.place_buy_stock_limit_order("QQQ",10,379)
    trade.fillEvent += order_filled
    ib.sleep(5)
    ib.run()


if __name__ == "__main__":
    main()