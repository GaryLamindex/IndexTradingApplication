from ib_insync import *

ib = IB()
ib.connect("127.0.0.1",7497,clientId=1)

# stock = Stock("QQQ","SMART","USD")

# order = LimitOrder("BUY",10,383.95)

# trade = ib.placeOrder(stock,order)

# def orderFilled(trade,fill):
#     print("Order has been filled")
#     print(trade)
#     print(fill)

# trade.fillEvent += orderFilled

# ib.run()

# print(trade) # isn't necessarily filled yet !

# you can put all the order filled to a database with the callback function

# print(ib.trades())
# print(ib.orders())

for trade in ib.trades():
    print("== this is one of my trades ==")
    print(trade)

for order in ib.orders():
    print("== this is one of my order ==")
    print(order)