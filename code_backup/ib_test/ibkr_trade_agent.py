from ib_insync import *

class ibkr_trade_agent:
    ib_instance = None
    
    # pass in an initialized IB object to instantiate the trade agent object
    def __init__(self,ib_instance):
        self.ib_instance = ib_instance

    # once the order is filled, perform some task
    def order_filled(trade, fill):
        pass

    # place the order and return the trades
    # assuming all the trades are related to stocks
    def place_buy_stock_mkt_order(self,ticker,share_purchase):
        # create the stock object
        stock = Stock(ticker,"SMART","USD")
        # create the order
        order = MarketOrder("BUY",share_purchase)
        self.ib_instance.qualifyContracts(stock)
        # place and print the details of the order
        trade = self.ib_instance.placeOrder(stock,order)
        # trade.fillEvent += self.order_filled
        print(trade) 
        return trade

    def place_buy_stock_limit_order(self,ticker,share_purchase,limit_price):
        # create the stock objct
        stock = Stock(ticker,"SMART","USD")
        # create the order
        order = LimitOrder("BUY",share_purchase,limit_price,outsideRth=True)
        self.ib_instance.qualifyContracts(stock)
        # place and print the details of the order
        trade = self.ib_instance.placeOrder(stock,order)
        # trade.fillEvent += self.order_filled
        print(trade)
        return trade
    
    def place_sell_stock_mkt_order(self,ticker,share_sell):
        # create the stock object
        stock = Stock(ticker,"SMART","USD")
        # create the order
        order = MarketOrder("SELL",share_sell)
        self.ib_instance.qualifyContracts(stock)
        # place and print the details of the order
        trade = self.ib_instance.placeOrder(stock,order)
        # trade.fillEvent += self.order_filled
        print(trade)
        return trade

    def place_sell_stock_limit_order(self,ticker,share_sell,limit_price):
        # create the stock object
        stock = Stock(ticker,"SMART","USD")
        # create the order
        order = LimitOrder("SELL",share_sell,limit_price,outsideRth=True)
        self.ib_instance.qualifyContracts(stock)
        # place and print the details of the order
        trade = self.ib_instance.placeOrder(stock,order)
        # trade.fillEvent += self.order_filled
        print(trade)
        return trade

def main():
    ib = IB()
    ib.connect("127.0.0.1",7497,clientId=1)
    trade_agent = ibkr_trade_agent(ib)
    # trade_agent.place_buy_stock_mkt_order("QQQ",10)
    trade_agent.place_buy_stock_limit_order("QQQ",5,388)
    # print("trades:")
    # print(ib.trades())
    # # print("orders:")
    # print(ib.orders())
    # # while True:
    # #     stock = input("Stock: ")
    # #     type = input("Type: ")
    # ib.run()

if __name__ == "__main__":
    main()