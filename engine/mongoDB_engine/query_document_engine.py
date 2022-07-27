import json

from pymongo import MongoClient
import certifi
import requests
import pandas as pd
import datetime

# TradeLog grab document
class Grab_Mongodb:
    nft_flask = 'nft-flask'
    TradeLog = 'TradeLog'
    simulation = 'simulation'
    rainydrop = 'rainydrop'
    Strategies = 'Strategies'

    def __init__(self, run_data_df=None, all_file_return_df=None, drawdown_abstract_df=None, drawdown_raw_df=None):
        self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=certifi.where())
        print(f"Successful connection to mongoClient")
        self.rainydrop_db = self.conn[self.rainydrop]
        self.nft_db = self.conn[self.nft_flask]
        self.run_data_df = run_data_df
        self.all_file_return_df = all_file_return_df
        self.drawdown_abstract_df = drawdown_abstract_df
        self.drawdown_raw_df = drawdown_raw_df
        self.mongo = None
