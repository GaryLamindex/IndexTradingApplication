from time import sleep
import sys
import pathlib
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
    ibkr_trade_engine = ibkr_trade_agent(ib)
    ibkr_stock_data_engine = ibkr_stock_data_io_engine(ib)

    # take a snapshot of the portfolio engine first
    ibkr_portfolio_engine.update_stock_price_and_portfolio_data()
    write_account_record(ibkr_portfolio_engine)
    sleep(5)

    while True:

        if ibkr_trade_engine.market_opened():
            # fetch real time price
            real_time_ticker_price = ibkr_stock_data_engine.get_ibkr_open_price(tickers)
            print(real_time_ticker_price)

            # testing valid buy limit order 
            for ticker in tickers:
                ticker_price = real_time_ticker_price.get(ticker)["last"] # get the last price
                action_msg = ibkr_trade_engine.place_buy_stock_limit_order(ticker, 1, ticker_price*(1+acceptance_range))
                print("action_msg:", action_msg)
                write_transaction_record(action_msg)

            ib.sleep(1) # update the ib instance

            ibkr_portfolio_engine.update_stock_price_and_portfolio_data() # update the ibkr_acc object
            write_account_record(ibkr_portfolio_engine) # take a snpashot of the ibkr_acc object
            sleep(10)

            # testing valid sell limit order
            real_time_ticker_price = ibkr_stock_data_engine.get_ibkr_open_price(tickers)
            print(real_time_ticker_price)
            for ticker in tickers:
                ticker_price = real_time_ticker_price.get(ticker)["last"] # get the last price
                action_msg = ibkr_trade_engine.place_sell_stock_limit_order(ticker, 1, ticker_price*(1-acceptance_range))
                print("action_msg:", action_msg)
                write_transaction_record(action_msg)

            ib.sleep(1) # update the ib instance

            ibkr_portfolio_engine.update_stock_price_and_portfolio_data() # update the ibkr_acc object
            write_account_record(ibkr_portfolio_engine) # take a snpashot of the ibkr_acc object

            sleep(10)
            # testing invalid limit order
            # expect to automatically cancel the order after 60 s
            real_time_ticker_price = ibkr_stock_data_engine.get_ibkr_open_price(tickers)
            print(real_time_ticker_price)
            for ticker in tickers:
                ticker_price = real_time_ticker_price.get(ticker)["last"] # get the last price
                action_msg = ibkr_trade_engine.place_buy_stock_limit_order(ticker, 1, ticker_price * 0.8)
                print("action_msg:", action_msg)
                write_transaction_record(action_msg)

            ib.sleep(1) # update the ib instance
            
            ibkr_portfolio_engine.update_stock_price_and_portfolio_data()
            write_account_record(ibkr_portfolio_engine) # take a snpashot of the ibkr_acc object

            print("==========\nFinished One Loop\n==========")

if __name__ == "__main__":
    main()