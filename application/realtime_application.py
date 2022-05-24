from ib_insync import *

import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.resolve()))

from pythonProject.object.ibkr_acc_data import *
from pythonProject.engine.realtime_engine_ibkr.portfolio_data_engine import *
from pythonProject.engine.realtime_engine_ibkr.trade_engine import *
from pythonProject.engine.realtime_engine_ibkr.stock_data_engine import *
from pythonProject.algo.rebalance_margin_wif_max_drawdown_control import algorithm
from pythonProject.engine.simulation_engine.simulation_agent import *

# def write_account_data(portfolio_obj):
#     account_record_field_name = ['date','timestamp','TotalCashValue','NetDividend','NetLiquidation','UnrealizedPnL','RealizedPnL','GrossPositionValue','AvailableFunds','ExcessLiquidity',
#     'BuyingPower','Leverage','EquityWithLoanValue','QQQ position','QQQ marketPrice','QQQ averageCost','QQQ marketValue','QQQ realizedPNL','QQQ unrealizedPNL','QQQ initMarginReq','QQQ maintMarginReq','QQQ costBasis',
#     'SPY position','SPY marketPrice','SPY averageCost','SPY marketValue','SPY realizedPNL','SPY unrealizedPNL','SPY initMarginReq','SPY maintMarginReq','SPY costBasis']
#
#     output_path = str(pathlib.Path(__file__).parent.parent.resolve()) + "/real_time_data"
#
#     account_snapshot_dict = portfolio_obj.get_account_snapshot()
#     if "account_record.csv" not in os.listdir(output_path):
#         with open(f"{output_path}/account_record.csv",'a+',newline='') as f:
#             writer = csv.DictWriter(f,account_record_field_name)
#             writer.writeheader()
#             writer.writerow(account_snapshot_dict)
#     else:
#         with open(f"{output_path}/account_record.csv",'a+',newline='') as f:
#             writer = csv.DictWriter(f,account_record_field_name)
#             writer.writerow(account_snapshot_dict)

def main():
    # tickers = ["QQQ", "SPY"]
    tickers = ['QQQ','SPY']
    acceptance_range = 0.02 # for placing limit order
    max_drawdown_ratio = 0.04 # to be modified
    rebalance_margin = 0.038 # to be modified
    maintain_margin = 0.01
    purchase_exliq = 5.0
    # create ibkr_acc_data object
    user_id = 0
    strategy = "rebalance_margin_wif_max_drawdown_control"
    realtime_spec = {"rebalance_margin": rebalance_margin, "maintain_margin": maintain_margin,
                     "max_drawdown_ratio": max_drawdown_ratio, "purchase_exliq": purchase_exliq}
    spec_str = ""
    for k, v in realtime_spec.items():
        spec_str = f"{spec_str}{str(v)}_{str(k)}_"

    table_info = {"mode": "realtime", "strategy_name": strategy, "user_id": user_id}
    # strategy_name = "rebalance_margin_wif_max_drawdown_control"
    table_name = table_info.get("mode") + "_" + table_info.get("strategy_name") + "_" + str(table_info.get("user_id"))
    ibkr_acc = ibkr_acc_data(table_info.get("user_id"), table_info.get("strategy_name"), table_name, spec_str)


    # instantiate the ib object and connection
    ib = IB()
    ib.connect('127.0.0.1',7497,clientId=1)

    ibkr_portfolio_engine = ibkr_portfolio_data_engine(ibkr_acc,ib)
    ibkr_trade_engine = ibkr_trade_agent(ib)
    ibkr_stock_data_engine = ibkr_stock_data_io_engine(ib)
    sim_agent = simulation_agent(realtime_spec, table_info, False, ibkr_portfolio_engine, tickers)

    algo = algorithm.rebalance_margin_wif_max_drawdown(ibkr_trade_engine,ibkr_portfolio_engine,tickers,max_drawdown_ratio,acceptance_range,rebalance_margin)

    # before running the algorithm, take a snapshot of the portfolio
    ibkr_portfolio_engine.update_stock_price_and_portfolio_data()

    while True:
        stock_data_dict = ibkr_stock_data_engine.get_ibkr_open_price(tickers)
        action_messages = algo.run(stock_data_dict,dt.datetime.now().timestamp()) # the timestamp parameter is useless for realtime
        # record the account snapshot after running through the algorithm once
        ibkr_portfolio_engine.update_stock_price_and_portfolio_data()
        sim_agent.append_run_data_to_db(dt.datetime.now().timestamp(),action_messages)
        # sim_agent.write_transaction_record(action_messages)
        sim_agent.write_acc_data()
        # sleep 60 s
        sleep(60)

if __name__ == "__main__":
    main()

# ibkr_portfolio_data_engine ->