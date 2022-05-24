from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from threading import Timer

class TestApp(EWrapper,EClient):
    def __init__(self):
        EClient.__init__(self,self)

    def error(self, reqId, errorCode, errorString):
        print("Error: ", reqId, " ", errorCode, " ", errorString)

    def tickPrice(self, reqId, tickType, price, attrib):
        print("Tick Price. Tick Id:", reqId, "tickType:", TickTypeEnum.to_str(tickType), "Price:", price, end=' ')

    def tickSize(self, reqId, tickType, size):
        print("Tick Size. Ticker Id:", reqId, "tickType", TickTypeEnum.to_str(tickType), "Size:", size)

    # def contractDetails(self, reqId, contractDetails):
    #     print("contractDetails: ", reqId, " ", contractDetails)

    # def nextValidId(self, orderId):
    #     self.nextOrderId = orderId
    #     self.start()

    # def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFiillPrice, clientId, whyBeld, mktCapPrice):
    #     print("Orderstatus Id: ", orderId, ", Status: ", status, ", Filled: ", filled, ", Remaining: ", remaining, ", LastFillPrice: ", lastFiillPrice)

    # def openOrder(self, orderId, contract, order, orderState):
    #     print("OpendOrder ID: ", orderId, contract.symbol, contract.secType, "@", contract.exchange, ":", order.ation, order.orderType, order.totalQuantity, order.status)

    # def execDetails(self, reqId, contract, execution):
    #     print("ExecDetails. ", reqId, contract.symbol, contract.secType, contract.currency, execution.execId, execution.orderId, execution.shares, execution.lastLiquidity)

    # def start(self,symbol,quantity,limit_Price):
    #     contract = Contract()
    #     contract.symbol = symbol
    #     contract.secType = "STK"
    #     contract.exchange = "SMART"
    #     contract.currency = "USD"
    #     contract.primaryExchange = "NASDAQ"

    #     order = Order()
    #     order.action = "BUY"
    #     order.totalQuantity = quantity
    #     order.orderType = "LMT"
    #     order.lmtPrice = limit_Price

    #     self.placeOrder(self.nextOrderId, contract, order)

    # def stop(self):
    #     self.done = True
    #     self.disconnect()
    
def main():
    app = TestApp()
    app.connect("127.0.0.1",7497,0)

    contract = Contract()
    contract.symbol = "QQQ"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    contract.primaryExchange = "NASDAQ"

    app.reqMarketDataType(4)
    app.reqMktData(1,contract,"",False,False,[]) 

    app.run()

if __name__ == "__main__":
    main()