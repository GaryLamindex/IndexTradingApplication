from ib_insync import *
import datetime as dt
from time import sleep
import csv
import os
import pathlib
import math

import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()))

from failure_handler import connection_handler, connect_tws

class ibkr_trade_agent:
    ib_instance = None
    
    # pass in an initialized IB object to instantiate the trade agent object
    def __init__(self,ib_instance):
        self.ib_instance = ib_instance

    """
    Sampele of the message received if the market is not yet opened (all the bid ask info will not present):
    [Ticker(contract=Stock(conId=320227571, symbol='QQQ', exchange='SMART', 
    primaryExchange='NASDAQ', currency='USD', localSymbol='QQQ', tradingClass='NMS'), 
    time=datetime.datetime(2022, 1, 22, 12, 16, 13, 920097, tzinfo=datetime.timezone.utc), 
    bid=-1.0, bidSize=0.0, ask=-1.0, askSize=0.0, close=361.72, halted=0.0)]
    """
    # for simplicity, just check the message of QQQ
    @connection_handler
    def market_opened(self):
        connect_tws(self.ib_instance)
        contract = Stock("QQQ","SMART","USD")
        self.ib_instance.qualifyContracts(contract)
        self.ib_instance.reqMktData(contract,'',False,False)
        self.ib_instance.reqMarketDataType(marketDataType=1) # real time data
        data = self.ib_instance.reqTickers(contract)
        self.ib_instance.sleep(1)
        print("data:",data)
        if (data[0].bid == -1.0 and data[0].bidSize == 0 and data[0].ask == -1.0 and data[0].askSize == 0) or math.isnan(data[0].bid) or math.isnan(data[0].bidSize) or math.isnan(data[0].ask) or math.isnan(data[0].askSize):
            print("Market Closed !")
            return False
        else:
            print("Market Opened !")
            return True

    @connection_handler
    def fetch_trade_details(self,trade,type):
        connect_tws(self.ib_instance)
        # get the id of the currently placed trade
        current_id = trade.order.orderId
        quantity = trade.order.totalQuantity
        # continuously loop through the trade list and inspect the order status
        # return message: 
        # (i) limit order - state, timestamp, orderId, ticker, action, lmtPrice, totalQuantity, (avgPrice）, (error message), (exchange), (commission)
        # (ii) market order - state, timestamp, orderId, ticker, action, totalQuantity, (avgPrice）, (error message), (exchange), (commission)
        start_time = dt.datetime.now()
        while True:
            self.ib_instance.sleep(0) # making the ib_instance up to date
            current_time = dt.datetime.now()
            timestamp = current_time.timestamp()
            if trade.isDone(): # trade.isDone() method returns True if the trade is either fully filled or cancelled
                if trade.filled() == quantity: # fully filled
                    trade_list = self.ib_instance.trades()
                    for trade in trade_list:
                        if trade.order.orderId == current_id:
                            if type == "limit":
                                action_msg = {"state":"1","timestamp":timestamp,"orderId":current_id,"ticker":trade.contract.symbol,"action":trade.order.action,"lmtPrice":trade.order.lmtPrice,"totalQuantity":trade.order.totalQuantity,"avgPrice":trade.fills[-1].execution.avgPrice,"exchange":trade.fills[-1].execution.exchange,"commission":f"{trade.fills[-1].commissionReport.commission} {trade.fills[-1].commissionReport.currency}"}
                            elif type == "market":
                                action_msg = {"state":"1","timestamp":timestamp,"orderId":current_id,"ticker":trade.contract.symbol,"action":trade.order.action,"totalQuantity":trade.order.totalQuantity,"avgPrice":trade.fills[-1].execution.avgPrice,"exchange":trade.fills[-1].execution.exchange,"commission":f"{trade.fills[-1].commissionReport.commission} {trade.fills[-1].commissionReport.currency}"}
                            return action_msg
                else: # trade cancelled
                    if type == "limit":
                        action_msg = {"state":"0","timestamp":timestamp,"orderId":current_id,"ticker":trade.contract.symbol,"action":trade.order.action,"lmtPrice":trade.order.lmtPrice,"totalQuantity":trade.order.totalQuantity,"error message":trade.log[-1].message}
                    elif type == "market":
                        action_msg = {"state":"0","timestamp":timestamp,"orderId":current_id,"ticker":trade.contract.symbol,"action":trade.order.action,"totalQuantity":trade.order.totalQuantity,"error message":trade.log[-1].message}
                    return action_msg

            # if already fetched for 60 seconds, but the order is not yet filled / cancelled -> cancel the order
            if ((current_time - start_time).total_seconds() > 60):
                self.ib_instance.cancelOrder(trade.order)
                if type == "limit":
                    action_msg = {"state":"0","timestamp":timestamp,"orderId":current_id,"ticker":trade.contract.symbol,"action":trade.order.action,"lmtPrice":trade.order.lmtPrice,"totalQuantity":trade.order.totalQuantity,"error message":"Order cancelled: not filled within 60 seconds"}
                elif type == "market":
                    action_msg = {"state":"0","timestamp":timestamp,"orderId":current_id,"ticker":trade.contract.symbol,"action":trade.order.action,"totalQuantity":trade.order.totalQuantity,"error message":"Order cancelled: not filled within 60 seconds"}
                return action_msg
    """
    limit order
    sample return output 1 (success): 
    sample return ouptut 2 (fail): 

    market order
    sample return output 1 (success): 
    sample return ouptut 2 (fail): 
    """

    # helper function with error handlers
    # action: "BUY" or "SELL"
    # NOT called directly
    @connection_handler
    def place_mkt_order(self,ticker,quantity,action):
        connect_tws(self.ib_instance)
        # create the stock object
        stock = Stock(ticker,"SMART","USD")
        # qualify the contract
        self.ib_instance.qualifyContracts(stock)
        # create the order
        order = MarketOrder(action,quantity)
        # place and print the details of the order
        trade = self.ib_instance.placeOrder(stock,order)
        self.ib_instance.sleep(1) # wait several seconds for the trade to register
        print(f"{action} market order placed,", trade)
        return trade

    @connection_handler
    def place_limit_order(self,ticker,quantity,limit_price,action):
        connect_tws(self.ib_instance)
        # create the stock object
        stock = Stock(ticker,"SMART","USD")
        # qualify the contract
        self.ib_instance.qualifyContracts(stock)
        # create the order
        order = LimitOrder(action,quantity,round(limit_price,1),outsideRth=True)
        # place and print the details of the order
        trade = self.ib_instance.placeOrder(stock,order)
        self.ib_instance.sleep(1) # wait several seconds for the trade to register
        print(f"{action} limit order placed,", trade)
        return trade
    
    # the COMPLETE order function
    # TO BE CALLED DIRECTLY
    # place the order the fetch details 
    def place_buy_stock_mkt_order(self,ticker,share_purchase,*args): # to be modified
        trade = self.place_mkt_order(ticker,share_purchase,"BUY")
        return self.fetch_trade_details(trade,"market")

    def place_buy_stock_limit_order(self,ticker,share_purchase,limit_price,*args):
        trade = self.place_limit_order(ticker,share_purchase,limit_price,"BUY")
        return self.fetch_trade_details(trade,"limit")
    
    def place_sell_stock_mkt_order(self,ticker,share_sold,*args): # to be modified
        trade = self.place_mkt_order(ticker,share_sold,"SELL")
        return self.fetch_trade_details(trade,"market")

    def place_sell_stock_limit_order(self,ticker,share_sold,limit_price,*args):
        trade = self.place_limit_order(ticker,share_sold,limit_price,"SELL")
        return self.fetch_trade_details(trade,"limit")

    # write the record to the file "transaction_record.csv" in the outermost directory
    def append_stock_transaction_record(self,action_message):
        output_filepath = str(pathlib.Path(__file__).parent.parent.parent.resolve()) + "/transaction_record.csv"
        fieldname = ["state","timestamp","orderId", "ticker","action","lmtPrice","totalQuantity","avgPrice","error message","exchange","commission"]
        if "transaction_record.csv" not in os.listdir(str(pathlib.Path(output_filepath).parent.resolve())):
            with open(output_filepath,'a+',newline='') as f:
                writer = csv.DictWriter(f,fieldname)
                writer.writeheader()
                writer.writerow(action_message)
        else:
            with open(output_filepath,'a+',newline='') as f:
                writer = csv.DictWriter(f,fieldname)
                writer.writerow(action_message)

        print("Trade record written successfully.")

def main():
    ib = IB()
    ib.connect("127.0.0.1",7497,clientId=1)
    ib.reqMarketDataType(marketDataType=1)
    trade_agent = ibkr_trade_agent(ib)
    # stock = Stock("QQQ","SMART","USD")
    # order = LimitOrder("BUY",10,350,outsideRth=True)
    # trade = ib.placeOrder(stock,order)
    # for i in range(100):
    #     print(trade)
    #     sleep(1)
    #     print(ib.trades()[0])
    #     sleep(1)
    # ib.sleep(2)
    # print(ib.trades())
    # trade_agent.place_buy_stock_mkt_order("QQQ",10)
    # trade_agent.place_buy_stock_limit_order("QQQ",2,350.111)
    # print(trade_agent.place_sell_stock_mkt_order("QQQ",2))
    # ib.sleep(0)
    # print(trade_agent.place_sell_stock_limit_order("QQQ",2,355.111111)) # wrong limit price
    # ib.sleep(0)
    # print(trade_agent.place_sell_stock_limit_order("QQQ",2,390)) # limit price is too high, I will cancel the order
    # ib.sleep(0)
    # print(trade_agent.place_sell_stock_limit_order("QQQ",2,370))
    # ib.sleep(0)
    # trade_agent.place_sell_stock_limit_order("QQQ",2,360)
    # trade_agent.place_buy_stock_mkt_order("QQQ",2)
    # order = LimitOrder(stock,10,)
    # print("trades:")
    # print(ib.trades())
    # # print("orders:")
    # print(ib.orders())
    # # while True:
    # #     stock = input("Stock: ")
    # #     type = input("Type: ")
    # ib.run()
    # while True:
    #     if not trade_agent.market_opened():
    #         print("Market closed !")
    #     else:
    #         print("Market opened !")
    #     sleep(10)
    # action_msg = trade_agent.place_sell_stock_limit_order("QQQ",1,340)
    # trade_agent.append_stock_transaction_record(action_msg)
    while True:
        if (trade_agent.market_opened()):
            print("Market opened !")
        else:
            print("Market close !")
        sleep(5)

if __name__ == "__main__":
    main()

"""
cancelled (automatically) trades will disapper once the ib instances disconnects
Five cases:
1. successfully submitted and filled
2. successfully submitted but only filled for a certain portion, not yet filled all, or successfully submitted but none is filled
3. successfully submitted but not yet filled for a long time - ignore it and wait until the trade is filled
4. successfully submitted but cancelled due to a certain misbehaviour
5. successfully submiited bub cancelled manually
"""