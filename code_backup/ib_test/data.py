from ib_insync import *
import nest_asyncio
nest_asyncio.apply()

from time import sleep

ib = IB()
ib.connect("127.0.0.1", 7497, clientId=1) # for IBgateway: 4001 / 4002 

# contract = Forex('EURUSD')
contract = Stock(symbol="SPY",exchange="SMART",currency="USD")
ib.qualifyContracts(contract)
# alternative: Stock("QQQ","SMART","USD")
# alternative: Contract("STK","QQQ","SMART","USD")

# useRTH: only show the data in regular trading hours 
# you can set it to FALSE !
# date = ib.reqHeadTimeStamp(contract,'MIDPOINT',False)
# print(date)
# print(date)
bars = ib.reqHistoricalData(contract,endDateTime='20110113 00:00:00',durationStr='1 M',barSizeSetting='1 min',whatToShow='MIDPOINT',useRTH=False)

# note that for real time data, the process if ASYNC
# need to wait for the callback (response) to come before you proceed the program
# ib.reqMarketDataType(1)
# market_data = ib.reqMktData(contract,'',False,False)
# def onPendingTicker(ticker):
#     print("Data received")
#     print(ticker)

# ib.pendingTickersEvent += onPendingTicker # adding a callback function to an event
# ib.run()
# convert into pandas df
df = util.df(bars)
print(df)
# print(df.index)
# for index in df.index:
#     print(df.loc[index]['open'])