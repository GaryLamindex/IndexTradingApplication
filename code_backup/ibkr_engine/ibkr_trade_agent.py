from datetime import date
from time import sleep

from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.opt import ibConnection, Connection
from ib.opt import ibConnection, message
from tinydb import TinyDB, Query

# from engine.ibkr_engine.ibkr_portfolio_data_agent import ibkr_portfolio_data_agent

class ibkr_trade_agent(object):
    path = ""
    db_path = ""
    nextorderId = 0

    def __init__(self, path):
        self.path = path
        self.db_path = path + "/db"

    def place_buy_stock_mkt_order(self, ticker, share_purchase, sim_medtadata):

        def error_handler(msg):
            if msg.errorCode == 201:
                print("Server Error: %s" % msg)

        def reply_handler(msg):
            """Handles of server replies"""
            print("Server Response: %s, %s" % (msg.typeName, msg))
            if msg.typeName == "orderStatus" and msg.status == "Filled" and self.fill_dict[msg.orderId]["filled"] == False:
                self.create_fill(msg)
            print("Server Response: %s, %s\n" % (msg.typeName, msg))

        def create_contract(symbol, sec_type, exch, prim_exch, curr):
            contract = Contract()
            contract.m_symbol = symbol
            contract.m_secType = sec_type
            contract.m_exchange = exch
            contract.m_primaryExch = prim_exch
            contract.m_currency = curr
            return contract

        def create_order(order_type, share_purchase, action):
            order = Order()
            order.m_orderType = order_type
            order.m_totalQuantity = share_purchase
            order.m_action = action
            return order

        #trade in IB
        conn = Connection.create(port=7496,clientId=100)
        conn.connect()
        conn.register(error_handler, 'Error')
        conn.registerAll(reply_handler)
        _contract = create_contract(ticker, 'STK', 'SMART', 'SMART', 'USD')

        # Go long 100 shares of Google
        _order = create_order('MKT', share_purchase, 'BUY')

        # Use the connection to the send the order to IB
        conn.placeOrder(2, _contract, _order)

        # Disconnect from TWS

        self.nextorderId += 1
        print("fully placed")

        print("")
        action_msg = self.check_mkt_order_status(conn)
        conn.disconnect()
        return action_msg
    
    def place_buy_stock_limit_order(self, ticker, share_purchase, limitPrice):
        def error_handler(msg):
            if msg.errorCode == 201:
                print("Server Error: %s" % msg)

        def reply_handler(msg):
            """Handles of server replies"""
            print("Server Response: %s, %s" % (msg.typeName, msg))
            if msg.typeName == "orderStatus" and msg.status == "Filled" and self.fill_dict[msg.orderId]["filled"] == False:
                self.create_fill(msg)
            print("Server Response: %s, %s\n" % (msg.typeName, msg))

        def create_contract(symbol, sec_type, exch, prim_exch, curr):
            contract = Contract()
            contract.m_symbol = symbol
            contract.m_secType = sec_type
            contract.m_exchange = exch
            contract.m_primaryExch = prim_exch
            contract.m_currency = curr
            return contract

        def create_order(order_type, share_purchase, action, limitPrice):
            order = Order()
            order.m_orderType = order_type
            order.m_totalQuantity = share_purchase
            order.m_action = action
            order.m_limitPrice = limitPrice
            return order
        
        #trade in IB
        conn = Connection.create(port=7496,clientId=100)
        conn.connect()
        conn.register(error_handler, 'Error')
        conn.registerAll(reply_handler)
        _contract = create_contract(ticker, 'STK', 'SMART', 'SMART', 'USD')

        # Go long 100 shares of Google
        _order = create_order('LMT', share_purchase, 'BUY', limitPrice)

        # Use the connection to the send the order to IB
        conn.placeOrder(3, _contract, _order)

        print("fully placed")

        print("")
        action_msg = self.check_mkt_order_status(conn)
        conn.disconnect()
        return action_msg

    def place_sell_stock_mkt_order(self, ticker, share_sell):

        def error_handler(msg):
            if msg.errorCode == 201:
                print("Server Error: %s" % msg)

        def reply_handler(msg):
            """Handles of server replies"""
            print("Server Response: %s, %s" % (msg.typeName, msg))

        def create_contract(symbol, sec_type, exch, prim_exch, curr):
            contract = Contract()
            contract.m_symbol = symbol
            contract.m_secType = sec_type
            contract.m_exchange = exch
            contract.m_primaryExch = prim_exch
            contract.m_currency = curr
            return contract

        def create_order(order_type, share_sell, action):
            order = Order()
            order.m_orderType = order_type
            order.m_totalQuantity = share_sell
            order.m_action = action
            return order

        trading_funds = TinyDB(self.db_path + '/trading_funds.json')
        stock_transaction_record = TinyDB(self.db_path + "/stock_transaction_record.json")
        margin_info = TinyDB(self.db_path + "/margin_info.json")

        #trade in IB
        conn = Connection.create(port=7496, clientId=100)
        conn.connect()

        conn.register(error_handler, 'Error')
        conn.registerAll(reply_handler)
        _contract = create_contract(ticker, 'STK', 'SMART', 'SMART', 'USD')

        # Go long 100 shares of Google
        _order = create_order('MKT', share_sell, 'SELL')

        # Use the connection to the send the order to IB
        conn.placeOrder(self.nextorderId, _contract, _order)
        sleep(1)
        # Disconnect from TWS
        conn.disconnect()
        self.nextorderId += 1
        print("fully placed")
        print("")

        pass

    def check_mkt_order_status(self, conn):
        action_msg = {}

        def print_open_order_messege(msg):
            print("open_order: " + str(msg.orderId) + "::" + str(msg.contract) + "::" + str(msg.order) + "::" + str(
                msg.orderState))

        def print_order_status_messege(msg):
            print("order_status: " + str(msg.orderId) + "::" + "Status: " + msg.status + ", Filled: " + str(
                msg.filled) + ", Remaining: " + str(msg.remaining) + ", avgFillPrice: " + str(msg.avgFillPrice))

        conn.register(print_open_order_messege, message.openOrder)
        conn.register(print_order_status_messege, message.orderStatus)
        conn.reqAllOpenOrders()

        return action_msg

def main():
    trade_agent = ibkr_trade_agent("")
    trade_agent.place_buy_stock_limit_order("QQQ", 10, 350)

if __name__ == "__main__":
    main()