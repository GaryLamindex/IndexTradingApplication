import math
import os
from datetime import datetime
from os import listdir
from pathlib import Path

import pytz
from ib.ext.Contract import Contract
from time import sleep, strftime, localtime
import pandas as pd
import numpy as np
from ib.opt import ibConnection, message
from tinydb import TinyDB
# import trading_calendars as tc

class ibkr_stock_data_agent(object):
    i = 0
    path = ""
    db_path = ""
    real_time_db_path = ""
    real_time_data_db = None
    his_data_db = None
    _real_time_data = {}

    def __init__(self, path):
        self.path = path
        self.real_time_db_path = path + "/real_time/db"

    def wipe_data(self, path):
        os.makedirs(path, exist_ok=True)
        list_of_sim_files_db = listdir(path)
        for file in list_of_sim_files_db:
            os.remove(Path(path +'/' + file))

    def query_his_data(self, tickers, startTime, endTime, dataFreq):
        dict_array = []

        def error_handler(msg):
            print("Server Error: %s" % msg)

        def historical_data_handler(msg):
            print(msg)
            print("historical_data_handler")
            # print msg.reqId, msg.date, msg.open, msg.high, msg.low, msg.close, msg.volume
            if ('finished' in str(msg.date)) == False:
                timestamp = msg.date
                _date = datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
                _time = datetime.fromtimestamp(int(timestamp)).strftime("%H:%M:%S")
                print('#' * 20, _date, ":", _time, '#' * 20)

                dict = {"timestamp": msg.date, "date":_date, "time":_time, "open": msg.open, "high": msg.high, "low": msg.low,
                        "close": msg.close, "volume": msg.volume, "count": msg.count, "WAP": msg.WAP}
                self.his_data_db.insert(dict)
                print(dict)
            else:
                print("finished")

        con = ibConnection()
        con.register(historical_data_handler, message.historicalData)
        con.register(error_handler, 'Error')

        con.connect()

        symbol_id = 0
        for ticker in tickers:
            print("symbol_id:",symbol_id,": ",ticker)
            self.his_data_db = TinyDB(self.path+'/'+dataFreq+"/"+ticker+".json")

            contract = Contract()
            contract.m_symbol = ticker
            contract.m_secType = 'STK'
            contract.m_exchange = 'SMART'
            contract.m_currency = 'USD'

            delta = endTime - startTime
            days = delta.days
            print("days:",days)
            if days > 365:
                yrs = days/365
                ceil_yr = math.floor(yrs)
                duration_str = str(ceil_yr)+" Y"
            else:
                duration_str = str(days) + " D"

            if "min" in dataFreq:
                entry_freq = 60 / int(dataFreq.split()[0])
                sleep_sec = days * 9 * entry_freq / 3
                print("sleep_sec:", sleep_sec)

            endTime = endTime.strftime('%Y%m%d %H:%M:%S')
            print("endTime:",endTime,"; duration_str:",duration_str,"; dataFreq:",dataFreq)
            con.reqHistoricalData(symbol_id, contract, endTime, duration_str, dataFreq, 'TRADES', 1, 2)

            symbol_id = symbol_id + 1
            #must sleep enough time for conn to grap data, if
            sleep(sleep_sec)

    def query_real_time_data(self, tickers):

        def error_handler(msg):
            print("Server Error: %s" % msg)

        def reply_handler(msg):
            print("Server Response: %s, %s" % (msg.typeName, msg))

        def handleAll(msg):
            print(msg)

        def real_time_data_handler(msg):
            print(msg)
            # print msg.reqId, msg.date, msg.open, msg.high, msg.low, msg.close, msg.volume

            if msg.field == 0:
                print("bid_size:", self._real_time_data)
                self._real_time_data.update({"bid_size":msg.size})
            elif msg.field == 1:
                self._real_time_data.update({"bid_price":msg.price})
            elif msg.field == 2:
                self._real_time_data.update({"ask_price":msg.price})
            elif msg.field == 3:
                self._real_time_data.update({"ask_size":msg.size})
            elif msg.field == 4:
                self._real_time_data.update({"last_price":msg.price})
            elif msg.field == 5:
                self._real_time_data.update({"last_size":msg.price})
            elif msg.field == 6:
                self._real_time_data.update({"high":msg.price})
            elif msg.field == 7:
                self._real_time_data.update({"low":msg.price})
            elif msg.field == 8:
                self._real_time_data.update({"volume":msg.size})
            elif msg.field == 9:
                self._real_time_data.update({"close_price":msg.price})
            elif msg.field == 14:
                self._real_time_data.update({"open_ticket":msg.price})
            elif msg.field == 45:
                self._real_time_data.update({"last_timestamp":datetime.fromtimestamp(int(msg.value)).strftime("%Y-%m-%d")})

        con = ibConnection()
        con.registerAll(real_time_data_handler)
        con.register(error_handler, 'Error')
        con.connect()

        symbol_id = 0
        for ticker in tickers:
            print(symbol_id,":",ticker)
            self.real_time_db = TinyDB(self.real_time_db_path + '/' + ticker + ".json")
            contract = Contract()
            contract.m_symbol = ticker
            contract.m_secType = 'STK'
            contract.m_exchange = 'SMART'
            contract.m_currency = 'USD'
            con.reqMktData(symbol_id, contract,'', False)
            sleep(1)
            print("_real_time_data:",self._real_time_data)
            self.real_time_db.insert(self._real_time_data)
            symbol_id = symbol_id + 1
            #must sleep enough time for conn to grap data, if

        return self._real_time_data

def main():
    agent = ibkr_stock_data_agent("")

if __name__ == "__main__":
    main()