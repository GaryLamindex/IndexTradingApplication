import json
from datetime import datetime

import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()))
import os
import csv
import datetime as dt

from engine.realtime_engine_ibkr.portfolio_data_engine import *
from engine.simulation_engine import sim_data_io_engine


class simulation_agent(object):
    spec = {}
    spec_str = ""
    run_file_path = ""
    table_info = {}
    tickers = []

    sim_data_engine = None
    portfolio_data_engine = None

    # example
    # spec:{"rebalance_margin":rebalance_margin,"maintain_margin":maintain_margin,"max_drawdown_ratio":max_drawdown_ratio,"purchase_exliq":purchase_exliq}
    # table_info:{"mode":"backtest","strategy_name":"rebalance_margin_wif_max_drawdown_control","user_id":0}
    # ??? portfolio agent: should be an initialized instance which is connect to TWS with an initialized ibkr acc object
    def __init__(self, spec, table_info, online, portfolio_data_engine, tickers):
        self.spec = spec
        self.table_info = table_info
        self.table_name = self.table_info.get("mode") + "_" + self.table_info.get("strategy_name") + "_" + str(
            self.table_info.get("user_id"))

        if online == True:
            self.sim_data_engine = sim_data_io_engine.online_engine()
        else:
            self.sim_data_engine = sim_data_io_engine.offline_engine(table_info)

        user_id = self.table_info.get("user_id")
        mode = table_info.get("mode")

        for k, v in spec.items():
            self.spec_str = f"{self.spec_str}{str(v)}_{str(k)}_"

        self.portfolio_data_engine = portfolio_data_engine
        self.acc_data = portfolio_data_engine.acc_data
        self.tickers = tickers

        # # initialize the attribute (column name) of the csv file
        # self.data_attribute = ['date', 'time' ,'timestamp','TotalCashValue','NetDividend','NetLiquidation','UnrealizedPnL','RealizedPnL','GrossPositionValue','AvailableFunds','ExcessLiquidity','BuyingPower','Leverage','EquityWithLoanValue']
        # for ticker in tickers:
        #     # overall position
        #     self.data_attribute += [f'{ticker} state',f'{ticker} position',f'{ticker} marketPrice',f'{ticker} averageCost',f'{ticker} marketValue',f'{ticker} realizedPNL',f'{ticker} unrealizedPNL',f'{ticker} initMarginReq',f'{ticker} maintMarginReq',f'{ticker} costBasis']
        # for ticker in tickers:
        #     # instance action
        #     self.data_attribute += [f'{ticker} action',f'{ticker} totalQuantity',f'{ticker} avgPrice',f'{ticker} commission', f'{ticker} transaction_amount']

        self.table_path = str(pathlib.Path(
            __file__).parent.parent.parent.parent.resolve()) + f"/user_id_{user_id}/{mode}/{self.table_name}"
        self.run_file_path = f"{self.table_path}/run_data/{self.spec_str}.csv"
        self.transaction_record_file_path = f"{self.table_path}/transaction_data/{self.spec_str}.csv"
        self.acc_data_file_path = f"{self.table_path}/acc_data/{self.spec_str}.csv"

    # no clear usage
    def get_net_action_dicts(self, action_msgs):
        print("action_msgs:", action_msgs)
        net_action_dicts = []
        for action_msg in action_msgs:
            action_ticker = action_msg.get('ticker')
            # if ticker not exist in net action
            if (net_action_dicts == None):
                action_type = action_msg.get('action')
                if (action_type == "BUY"):
                    action_ticker = action_msg.get('ticker')
                    action_amount = action_msg.get('avgPrice') * action_msg.get('totalQuantity')
                    net_action_dicts.append(
                        {action_ticker + ' action': action_type, action_ticker + " action amount": action_amount})

                elif (action_type == "SELL"):
                    action_ticker = action_msg.get('ticker')
                    action_amount = action_msg.get('transaction_amount')
                    net_action_dicts.append(
                        {action_ticker + ' action': action_type, action_ticker + " action amount": action_amount})

            elif any(action_ticker + ' action' in action_dict for action_dict in net_action_dicts):
                action_type = action_msg.get('action')
                print("action_type:", action_type)
                print("action_msg:", action_msg)
                action_amount = action_msg.get('transaction_amount')
                if action_type == "SELL":
                    action_amount = action_amount * -1
                    print("action_amount:", action_amount)
                previous_action_type = [action_dict[action_ticker + ' action'] for action_dict in net_action_dicts if
                                        action_ticker + ' action' in action_dict][0]
                previous_action_amount = \
                [action_dict[action_ticker + " action amount"] for action_dict in net_action_dicts if
                 action_ticker + ' action amount' in action_dict][0]
                if previous_action_type == 'SELL':
                    previous_action_amount = previous_action_amount * -1
                    print("previous_action_amount:", previous_action_amount)
                net_action_amount = action_amount + previous_action_amount
                if net_action_amount > 0:
                    net_action_dict = {action_ticker + ' action': "buy",
                                       action_ticker + " action amount": net_action_amount}
                else:
                    net_action_dict = {action_ticker + ' action': "sell",
                                       action_ticker + " action amount": net_action_amount * -1}

                net_action_dicts = [net_action_dict for net_action_dict in net_action_dicts if
                                    not action_ticker + ' action' in net_action_dict.keys()]
                net_action_dicts.append(net_action_dict)

            # if ticker already exist in net action, calculate the net action (buy+sell)
            else:
                action_type = action_msg.get('action')
                if (action_type == "buy"):
                    action_ticker = action_msg.get('ticker')
                    action_amount = action_msg.get('transaction_amount')
                    net_action_dicts.append(
                        {action_ticker + ' action': action_type, action_ticker + " action amount": action_amount})

                elif (action_type == "sell"):
                    action_ticker = action_msg.get('ticker')
                    action_amount = action_msg.get('transaction_amount')
                    net_action_dicts.append(
                        {action_ticker + ' action': action_type, action_ticker + " action amount": action_amount})

            print("net_action_dicts:", net_action_dicts)

        return net_action_dicts

    # def append_sim_data_to_db(self, timestamp, stock_data_dict, orig_db_snapshot_dict, updated_db_snapshot_dict, action_msgs):
    #     key_dict = {"timestamp":timestamp, "spec":self.spec_str}
    #
    #     action_dicts = self.get_net_action_dicts(action_msgs)
    #     _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
    #     _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
    #     date_time_dict = {"timestamp": timestamp,"date": _date, "time": _time}
    #     orig_db_dict = {str(key) + "(Day Start)": val for key, val in orig_db_snapshot_dict.items()}
    #     updated_db_dict = {str(key) + "(Day End)": val for key, val in updated_db_snapshot_dict.items()}
    #
    #     _sim_data = date_time_dict | stock_data_dict | orig_db_dict | updated_db_dict
    #     for action_dict in action_dicts:
    #         _sim_data = _sim_data | action_dict
    #
    #     upload_sim_dict = key_dict | _sim_data
    #     print("upload_sim_dict:",upload_sim_dict)
    #     self.sim_data_engine.upload_single_sim_data(self.spec_str, upload_sim_dict)

    def append_run_data_to_db(self, timestamp, orig_account_snapshot_dict, action_msgs, sim_meta_data, ticker_data):
        _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
        _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
        timestamp_dict = {"timestamp": timestamp, "date": _date, "time": _time}
        action_dicts = {}
        sim_data_res = {}
        ticker_data_res = {}
        for action_msg in action_msgs:
            temp_list = action_msg.copy()
            if not temp_list['action'] == 'rejected':
                #print("temp_list:", temp_list)
                action_ticker = temp_list["ticker"]
                try:
                    del action_msg[
                        'ticker']  # get rid of the "ticker" column, since the csv does NOT contain this attribute
                    del action_msg['orderId']
                    del action_msg['lmtPrice']
                    del action_msg['exchange']
                    del action_msg['timestamp']
                except KeyError:
                    pass

                action_res = {f"{action_ticker} {str(key)}": val for key, val in action_msg.items()}
                action_dicts.update(action_res)  # action_dicts|action_res
        print(action_dicts)

        try:
            del ticker_data['timestamp']
        except KeyError:
            pass
        sim_data_res = {}
        for ticker in self.tickers:
            #print("sim_meta_data[ticker].items()")
            #print(sim_meta_data[ticker].items())
            if len(sim_meta_data) > 0 and ticker in sim_meta_data:
                sim_data_res.update({f"{ticker} {str(key)}": val for key, val in sim_meta_data[ticker].items()})
            ticker_data_res.update({f"{ticker} mktPrice": ticker_data[ticker]['last']})
        print("sim_data_res")
        print(sim_data_res)
        run_dict = timestamp_dict | orig_account_snapshot_dict | ticker_data_res | sim_data_res | action_dicts
        self.data_attribute = run_dict.keys()

        # write the resulting_dict to a csv
        if f"{self.spec_str}.csv" not in os.listdir(f"{self.table_path}/run_data/"):
            with open(self.run_file_path, 'a+', newline='') as f:
                writer = csv.DictWriter(f, self.data_attribute)
                writer.writeheader()
                # writer.writerow(run_dict)
                writer.writerow(run_dict)
        else:
            with open(self.run_file_path, 'a+', newline='') as f:
                writer = csv.DictWriter(f, self.data_attribute)
                # writer.writerow(run_dict)
                writer.writerow(run_dict)

    def write_transaction_record(self, action_msgs):
        transaction_field_name = ["state", "timestamp", "orderId", "ticker", "action", "lmtPrice", "totalQuantity",
                                  "avgPrice", "error message", "exchange", "commission", "transaction_amount"]

        for action_msg in action_msgs:
            if f"{self.spec_str}.csv" not in os.listdir(f"{self.table_path}/transaction_data/"):
                with open(self.transaction_record_file_path, 'a+', newline='') as f:
                    writer = csv.DictWriter(f, transaction_field_name)
                    writer.writeheader()
                    writer.writerow(action_msg)
            else:
                with open(self.transaction_record_file_path, 'a+', newline='') as f:
                    writer = csv.DictWriter(f, transaction_field_name)
                    writer.writerow(action_msg)

    def write_acc_data(self):
        data_dict = self.acc_data.return_acc_data()
        acc_field_name = list(data_dict.keys())
        if f"{self.spec_str}.csv" not in os.listdir(f"{self.table_path}/acc_data/"):
            with open(self.acc_data_file_path, 'a+', newline='') as f:
                writer = csv.DictWriter(f, acc_field_name)
                writer.writeheader()
                writer.writerow(data_dict)
        else:
            with open(self.acc_data_file_path, 'a+', newline='') as f:
                writer = csv.DictWriter(f, acc_field_name)
                writer.writerow(data_dict)
        pass


def main():
    pass


if __name__ == "__main__":
    main()
