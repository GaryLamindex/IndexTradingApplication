"""
class hierarchy suggestion:
abstract base class "algorithm"
And then for any other specify algorithms (e.g., rebalance margin with max drawdown), inhereits the algorithm class and build addtional features
Now put everything together for simplicity, but better separate the base class and the child class
"""
import math
import sys
import pathlib

from pythonProject.engine.realtime_engine_ibkr.portfolio_data_engine import ibkr_portfolio_data_engine
from pythonProject.engine.realtime_engine_ibkr.stock_data_engine import ibkr_stock_data_io_engine
from pythonProject.engine.realtime_engine_ibkr.trade_engine import ibkr_trade_agent
from pythonProject.object.ibkr_acc_data import ibkr_acc_data

sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()))

from pythonProject.object.ibkr_acc_data import *
from pythonProject.engine.realtime_engine_ibkr.portfolio_data_engine import *
from pythonProject.engine.realtime_engine_ibkr.trade_engine import *
from pythonProject.engine.realtime_engine_ibkr.stock_data_engine import *

from ib_insync import *
from time import sleep

class rebalance_margin_wif_maintainance_margin:

    trade_agent = None
    portfolio_agent = None

    tickers = []
    number_of_stocks = 0

    purchase_exliq = 0

    rebalance_margin = 0
    maintain_margin = 0.01 # however, different stock may have different maintenance margin, just don't consider it here

    action_msgs = []

    loop = 0

    account_snapshot = {}

    def __init__(self,trade_agent,portfolio_agent, tickers,acceptance_range,rebalance_margin):
    
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.tickers = tickers
        self.number_of_stocks = len(tickers)

        self.acceptance_range = acceptance_range
        self.rebalance_margin = rebalance_margin

    # directly called to run the algorithm once
    def run(self,realtime_stock_data_dict, timestamp):
        # check if the market is opened, if not then do nothing and exit
        self.action_msgs = []
        if not self.trade_agent.market_opened():
            return
        
        # check if the data for the specify ticker exist, use temp_tickers instead of self.tickers for looping
        temp_ticker = []
        for ticker in self.tickers:
            if realtime_stock_data_dict.get(ticker) != None:
                temp_ticker.append(ticker)

        # update the portfolio data and get a snapshot of the account
        self.portfolio_agent.update_stock_price_and_portfolio_data(realtime_stock_data_dict)
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()

        # portfolio is empty, initialize the portfolio by buying equal of stock
        if (len(self.portfolio_agent.acc_data.portfolio) == 0):
            capital_for_each_stock = float(self.account_snapshot.get("TotalCashValue")) / self.number_of_stocks # or should use buying power ???
            for ticker in temp_ticker:
                ticker_price = realtime_stock_data_dict.get(ticker)['last']
                share_purchase = math.floor(capital_for_each_stock / ticker_price)
                action_msg = self.trade_agent.place_buy_stock_limit_order(ticker,share_purchase,ticker_price*(1+self.acceptance_range), timestamp)
                self.action_msgs.append(action_msg)

        else:
            for ticker in temp_ticker:
                target_ex_liq = self.rebalance_margin * float(self.account_snapshot.get("GrossPositionValue"))
                main_ex_liq = self.maintain_margin * float(self.account_snapshot.get("GrossPositionValue"))
                ticker_price = float(realtime_stock_data_dict.get(ticker)['last'])

                # if earned money and have excess liquidity, while the stock is not liquidated -> buy more stocks
                if float(self.account_snapshot.get("ExcessLiquidity")) > target_ex_liq:
                    ex_liq_diff = float(self.account_snapshot.get("ExcessLiquidity")) - target_ex_liq
                    target_share_purchase = math.floor(ex_liq_diff / ticker_price)

                    if (target_share_purchase > 0):
                        action_msg = self.trade_agent.place_buy_stock_limit_order(ticker,target_share_purchase,ticker_price*(1+self.acceptance_range), timestamp)
                        self.action_msgs.append(action_msg)
                    continue
                if float(self.account_snapshot.get("ExcessLiquidity")) < main_ex_liq:
                    ex_liq_diff =  target_ex_liq - float(self.account_snapshot.get("ExcessLiquidity"))
                    target_share_sell = math.floor(ex_liq_diff / ticker_price)
                    if (target_share_sell > 0):
                        action_msg = self.trade_agent.place_sell_stock_limit_order(ticker, target_share_sell, ticker_price*(1+self.acceptance_range), timestamp)
                        self.action_msgs.append(action_msg)

        self.loop+=1
        print(f"==========Finish running {self.loop} loop==========")
        return self.action_msgs.copy()


# def main():
#     tickers = ["QQQ", "SPY"]
#     acceptance_range = 0.02 # for placing limit order
#     max_drawdown_ratio = 0 # to be modified
#     rebalance_margin = 0 # to be modified
#
#     # create ibkr_acc_data object
#     user_id = 0
#     table_info = {"mode": "realtime", "strategy_name": "test", "user_id": user_id}
#     # strategy_name = "rebalance_margin_wif_max_drawdown_control"
#     table_name = table_info.get("mode") + "_" + table_info.get("strategy_name") + "_" + str(table_info.get("user_id"))
#     spec_str = "test"
#     ibkr_acc = ibkr_acc_data(table_info.get("user_id"), table_info.get("strategy_name"), table_name, spec_str)
#
#     # instantiate the ib object and connection
#     ib = IB()
#     ib.connect('127.0.0.1',7497,clientId=1)
#
#     ibkr_portfolio_engine = ibkr_portfolio_data_engine(ibkr_acc,ib)
#     ibkr_trade_engine = ibkr_trade_agent(ib)
#     ibkr_stock_data_engine = ibkr_stock_data_io_engine(ib)
#
#     algo = rebalance_margin_wif_max_drawdown(ibkr_trade_engine,ibkr_stock_data_engine,ibkr_portfolio_engine,tickers,max_drawdown_ratio,acceptance_range,rebalance_margin)
#
#     while True:
#         algo.run()
#         sleep(60)
#
# if __name__ == "__main__":
#     main()