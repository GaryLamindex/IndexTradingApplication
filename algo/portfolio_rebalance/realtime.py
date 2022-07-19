import numpy as np
import pandas as pd
import yfinance as yf


class realtime:
    pass

def main():
    data = yf.download(tickers='MSFT', period='1m', interval='1m')
    a=0

if __name__ == "__main__":
    main()
