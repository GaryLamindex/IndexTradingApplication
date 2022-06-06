import pandas as pd
import pathlib

class crypto_data_engine:
    def __init__(self):
        self.ticker_data_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.parent.resolve()) + '/ticker_data/one_min'
