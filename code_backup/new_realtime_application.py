import datetime as dt
import math
import sys
import pathlib
import csv
sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.resolve()))

from pythonProject.object.ibkr_acc_data import *
from pythonProject.engine.realtime_engine_ibkr.portfolio_data_engine import *
from pythonProject.engine.realtime_engine_ibkr.trade_engine import *
from pythonProject.engine.realtime_engine_ibkr.stock_data_engine import *

from ib_insync import *

# global variable
TRANSACTION_FIELD_NAME = ["state","timestamp","orderId", "ticker","action","lmtPrice","totalQuantity","avgPrice","error message","exchange","commission"]
ACCOUNT_RECORD_FIELD_NAME = ['date','timestamp','TotalCashValue','NetDividend','NetLiquidation','UnrealizedPnL','RealizedPnL','GrossPositionValue','AvailableFunds','ExcessLiquidity',
'BuyingPower','Leverage','EquityWithLoanValue','QQQ position','QQQ marketPrice','QQQ averageCost','QQQ marketValue','QQQ realizedPNL','QQQ unrealizedPNL','QQQ initMarginReq','QQQ maintMarginReq','QQQ costBasis',
'SPY position','SPY marketPrice','SPY averageCost','SPY marketValue','SPY realizedPNL','SPY unrealizedPNL','SPY initMarginReq','SPY maintMarginReq','SPY costBasis']
FILEPATH = str(pathlib.Path(__file__).parent.resolve())
OUTPUT_PATH = output_path = f"{str(pathlib.Path(FILEPATH).parent.resolve())}/real_time_data"

def write_transaction_record(action_msg):
    if "transaction_record.csv" not in os.listdir(OUTPUT_PATH):
        with open(f"{OUTPUT_PATH}/transaction_record.csv",'a+',newline='') as f:
            writer = csv.DictWriter(f,TRANSACTION_FIELD_NAME)
            writer.writeheader()
            writer.writerow(action_msg)
    else:
        with open(f"{OUTPUT_PATH}/transaction_record.csv",'a+',newline='') as f:
            writer = csv.DictWriter(f,TRANSACTION_FIELD_NAME)
            writer.writerow(action_msg)

def write_account_record(portfolio_obj):
    account_snapshot_dict = portfolio_obj.get_account_snapshot()
    if "account_record.csv" not in os.listdir(OUTPUT_PATH):
        with open(f"{OUTPUT_PATH}/account_record.csv",'a+',newline='') as f:
            writer = csv.DictWriter(f,ACCOUNT_RECORD_FIELD_NAME)
            writer.writeheader()
            writer.writerow(account_snapshot_dict)
    else:
        with open(f"{OUTPUT_PATH}/account_record.csv",'a+',newline='') as f:
            writer = csv.DictWriter(f,ACCOUNT_RECORD_FIELD_NAME)
            writer.writerow(account_snapshot_dict)

class algorithm:
    tickers = []

    number_of_stocks = 0

    trade_agent = None
    stock_data_agent = None
    portfolio_agent = None

    max_drawdown_ratio = 0
    purchase_exliq = 0

    max_drawdown_dodge = {} # a dictionary of boolean value, checking for each ticker

    max_stock_price = {}
    max_drawdown_price = {}

    """to be modified later"""
    rebalance_margin = 0
    maintain_margin = 0

    liq_sold_price_dict = {}
    liq_sold_price_qty_dict = {}

    def __init__(self,tickers,max_drawdown_ratio,trade_agent,stock_data_agent,portfolio_agent):
        self.tickers = tickers
        self.number_of_stocks = len(self.tickers)
        self.max_drawdown_ratio = max_drawdown_ratio
        self.trade_agent = trade_agent
        self.stock_data_agent = stock_data_agent
        self.portfolio_agent = portfolio_agent
    
        for ticker in self.tickers:
            self.max_stock_price.update({ticker:0})
            self.max_drawdown_price.update({ticker:0})
            self.max_drawdown_dodge.update({ticker:False})

    def run_algo(self,realtime_stock_data_dict):
        # update the max price and the drawdown price
        self.update_max_and_drawdown_price(realtime_stock_data_dict)

        # update the portfolio data and get a snapshot of the account
        self.portfolio_agent.update_stock_price_and_portfolio_data()
        account_snapshot = self.portfolio_agent.get_account_snapshot()

        # initialize the portfolio if the portfolio object is empty
        if (len(self.acc_data.portfolio) == 0):
            for ticker in self.tickers:
                ticker_price = realtime_stock_data_dict.get(ticker)
                no_of_tickers = len(realtime_stock_data_dict)
                ticker_purchase_amount = account_snapshot.get("TotalCashValue") / no_of_tickers
                share_purchase = math.floor(ticker_purchase_amount / ticker_price) # need to adjust the share purchase or not ??? (due to commision and bid ask spread)
                action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker,share_purchase) # market or limit ???
                write_transaction_record(action_msg)
        else:
            for ticker in self.tickers:
                ticker_price = realtime_stock_data_dict.get(ticker)
                if self.max_drawdown_dodge.get(ticker): # stock is previously liquidated
                    self.portfolio_agent.update_stock_price_and_portfolio_data() # update after buying
                    account_snapshot = self.portfolio_agent.get_account_snapshot() # update the accnount snapshot
                    buying_power = account_snapshot.get("BuyingPower")
                    
                    self.adjust_max_and_drawdown_price(ticker,ticker_price)

                    if self.check_buy_back(ticker,ticker_price):
                        action_msg = self.buy_back_position(ticker,ticker_price,buying_power)
                        write_transaction_record(action_msg)
                        self.max_drawdown_dodge.update({ticker:False})
                else:
                    if self.is_max_drawdown_dodge(ticker,ticker_price):
                        action_msg = self.liquidate_all_pos(ticker,ticker_price)
                        write_transaction_record(action_msg)
                    else:
                        target_ex_liq = self.rebalance_margin * account_snapshot.get("GrossPositionValue")
                        main_ex_liq = self.maintain_margin * account_snapshot.get("GrossPositionValue")

                        if (account_snapshot.get("ExcessLiquidity") > target_ex_liq):
                            ex_liq_diff = account_snapshot.get("ExcessLiquidity") - target_ex_liq
                            target_purchase_each = ex_liq_diff / self.number_of_stocks
                            target_share_purchase = math.floor(target_purchase_each / ticker_price)

                            if (target_share_purchase > 0):
                                action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker,target_share_purchase)
                                write_transaction_record(action_msg)
                        
                        elif (account_snapshot.get("ExcessLiquidity") < main_ex_liq): # facing liquidation
                            # liquidate the asset as soon as possible
                            ex_liq_diff = main_ex_liq - account_snapshot.get("ExcessLiquidity")
                            target_share_sell = math.ceil(ex_liq_diff / ticker_price)
                            if (account_snapshot.get(f"{ticker} position") != None):
                                if account_snapshot.get(f"{ticker} position") >= target_share_sell:
                                    action_msg = self.trade_agent.place_sell_stock_mkt_order(ticker,target_share_sell)
                                else:
                                    action_msg = self.trade_agent.place_sell_stock_mkt_order(ticker,account_snapshot.get(f"{ticker} position"))
                                write_transaction_record(action_msg)
            
        # write the account data after one loop
        self.portfolio_agent.update_stock_price_and_portfolio_data()
        write_account_record(self.portfolio_agent)

    def update_max_and_drawdown_price(self,realtime_stock_data_dict):
        for ticker in self.tickers:
            stock_price = realtime_stock_data_dict.get(ticker)['last']
            if (self.max_stock_price.get(ticker) == 0):
                self.max_stock_price.update({ticker:stock_price})
                drawdown_price = stock_price * (1 - self.max_drawdown_ratio)
                self.max_drawdown_price.update({ticker:drawdown_price})
            elif stock_price > self.max_stock_price.get(ticker):
                self.max_stock_price.update({ticker:stock_price})
                drawdown_price = stock_price * (1 - self.max_drawdown_ratio)
                self.max_drawdown_price.update({ticker:drawdown_price})
    
    def is_max_drawdown_dodge(self,ticker,ticker_price):
        ticker_max_drawdown_price = self.max_drawdown_price.get(ticker)
        return ticker_price < ticker_max_drawdown_price

    # for handling continuous downtrend
    def adjust_max_and_drawdown_price(self,ticker,ticker_price):
        if ticker_price < self.max_drawdown_price.get(ticker) * 0.9:
            self.max_stock_price.update({ticker:ticker_price})
            drawdown_price = ticker_price * (1 - self.max_drawdown_ratio)
            self.max_drawdown_price.update({ticker:drawdown_price})
    
    def check_buy_back(self,ticker,ticker_price):
        return ticker_price >= self.liq_sold_price_dict.get(ticker)

    def buy_back_position(self,ticker,ticker_price,buying_power):
        print(f"{ticker} ticker_price >= {ticker} liq_sold_price")
        liq_sold_price_qty = self.liq_sold_price_qty_dict.get(ticker)
        purchase_amount = liq_sold_price_qty * ticker_price
        if buying_power >= purchase_amount:
            action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker,liq_sold_price_qty)
        else:
            target_share_purchases = math.floor(buying_power / ticker_price)
            action_msg = self.trade_agent.place_buy_stock_mkt_order(ticker,target_share_purchases)
        
        write_transaction_record(action_msg)
    
    # for a particular stock
    def liquidate_all_pos(self,ticker,ticker_price):
        ticker_share_to_be_sold = self.portfolio_agent.get(f"{ticker} position")

        action_msg = self.trade_agent.place_sell_stock_mkt_order(ticker,ticker_share_to_be_sold)
        write_transaction_record(action_msg)

        self.liq_sold_price_dict.update({ticker:ticker_price})
        self.liq_sold_price_qty_dict.update({ticker:ticker_share_to_be_sold})

def main():
    # trade details
    tickers = ["QQQ", "SPY"]
    acceptance_range = 0.02

    # create ibkr_acc_data object
    user_id = 0
    table_info = {"mode": "realtime", "strategy_name": "test", "user_id": user_id}
    # strategy_name = "rebalance_margin_wif_max_drawdown_control"
    table_name = table_info.get("mode") + "_" + table_info.get("strategy_name") + "_" + str(table_info.get("user_id"))
    spec_str = "test"
    ibkr_acc = ibkr_acc_data(table_info.get("user_id"), table_info.get("strategy_name"), table_name, spec_str)

    # instantiate the ib object and connection
    ib = IB()
    ib.connect('127.0.0.1',7497,clientId=1)

    ibkr_portfolio_engine = ibkr_portfolio_data_engine(ibkr_acc,ib)

    ibkr_portfolio_engine.update_stock_price_and_portfolio_data()

    write_account_record(ibkr_portfolio_engine)

if __name__ == "__main__":
    main()