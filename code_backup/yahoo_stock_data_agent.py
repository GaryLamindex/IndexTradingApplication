import datetime
import json.decoder
import numpy as np
from tinydb import TinyDB, Query, where
import pandas as pd
from yahoofinancials import YahooFinancials
from engine.backtest_engine.data_calculation_engine import data_calculation_engine

class yahoo_stock_data_agent(object):

    path = ""
    db_path = ""
    data_calculation = None

    def __init__(self, path):
        self.path = path
        self.db_path = path + "/db"
        self.data_calculation = data_calculation_engine(path)

    def query_date_div(self, tickers, start_date, end_date):
        yahoo_financials = YahooFinancials(tickers)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        _div_dict = yahoo_financials.get_daily_dividend_data(start_date, end_date)
        print("_div_dict:",_div_dict)
        for ticker in tickers:
            amount = _div_dict.get(ticker)
            if amount is None:
                _div_dict.update({ticker: 0})
            else:
                _div_dict.update({ticker: amount})

        return _div_dict

    def query_nearest_5_days_div(self, tickers):
        yahoo_financials = YahooFinancials(tickers)
        today = datetime.datetime.today()
        start_date = today - datetime.timedelta(days=5)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
        _yahoo_div_dict = yahoo_financials.get_daily_dividend_data(start_date, end_date)
        _div_dict = {}
        print("_yahoo_div_dict:",_yahoo_div_dict)
        for ticker in tickers:
            _info = _yahoo_div_dict.get(ticker)
            if _info is None:
                _div_dict.update({ticker+" div amount": 0})
            else:
                _amount = _info[0].get("amount")
                _div_dict.update({ticker+" div amount": _amount})

        return _div_dict

    def query_real_time_single_data(self, ticker):
        yahoo_financials_stocks = YahooFinancials(ticker)
        _current_price_json = yahoo_financials_stocks.get_current_price()
        dict = json.load(_current_price_json)
        _price = dict[ticker]
        print(ticker,":" , _price)
        return _price

    def query_real_time_multiple_data(self, tickers):
        yahoo_financials_stocks = YahooFinancials(tickers)
        _current_price = yahoo_financials_stocks.get_current_price()
        return _current_price

    def query_his_data(self, tickers, start_date, end_date, dataFreq):
        _stock_db = TinyDB(self.db_path + "/stock_data.json")

        price_data = pd.DataFrame()
        div_data = pd.DataFrame()
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        for ticker in tickers:

            yahoo_financials = YahooFinancials(ticker)
            json_obj = yahoo_financials.get_historical_price_data(start_date, end_date, dataFreq)
            print(json_obj)
            ohlv0 = json_obj[ticker]['prices']
            temp0 = pd.DataFrame(ohlv0)[["formatted_date", "adjclose"]]
            # store dates in "Date" row (in the dictionary named close_prices)
            price_data["Date"] = temp0["formatted_date"]
            # store close prices of indexes in "SPY", "QQQ" and "VOO" rows
            price_data[ticker] = temp0["adjclose"].apply(np.ceil)
            price_data.dropna(axis=0, inplace=True)

            ohlv1 = json_obj[ticker]['eventsData']
            if ("dividends" in ohlv1):
                ohlv1 = ohlv1["dividends"]
                temp1 = pd.DataFrame(ohlv1).transpose()
                div_data[ticker+' div amount'] = temp1['amount']
            else:
                div_data[ticker + ' div amount'] = 0

        stock_data = pd.merge(div_data, price_data, left_index=True, right_on='Date', how="outer")
        stock_data['Date'] = pd.to_datetime(stock_data['Date'])
        stock_data.sort_values(by='Date', inplace=True, ascending=True)
        stock_data['Date'] = stock_data['Date'].dt.strftime('%Y-%m-%d')

        for ticker in tickers:
            stock_data[ticker+' div amount'] = stock_data[ticker+' div amount'].fillna(0)
        _stock_db.insert_multiple(stock_data.to_dict('records'))
        print("_stock_db")
        print(_stock_db.all())

    def update_portfolio_dividend(self, dividend_data_dict):

        portfolio = TinyDB(self.db_path + '/portfolio.json')
        mkt_value = TinyDB(self.db_path + '/mkt_value.json')

        tickers = [r['ticker'] for r in portfolio]
        print("dividend_data_dict:",dividend_data_dict)
        if len(tickers) > 0:
            print("Updating Portfolio Dividend")
            for ticker in tickers:
                # Check Dividends
                div_per_share = dividend_data_dict.get(ticker+" div amount")  # Retrieve stock data of date
                if div_per_share!= 0:
                    div = div_per_share * portfolio.get(Query().ticker == ticker).get('shares')
                    orig_div = mkt_value.all()[0].get("dividends")
                    orig_cash = mkt_value.all()[0].get("cash")
                    total_div = orig_div + div
                    total_cash = orig_cash + div
                    print("ticker:", ticker)
                    print("div_per_share:", div_per_share)
                    print("div:", div)
                    print("orig_cash:", orig_cash)
                    print("total_cash:", total_cash)
                    mkt_value.update({'dividends': total_div})
                    mkt_value.update({'cash': total_cash})

            print("total div:", mkt_value.all()[0].get("dividends"))
            self.data_calculation.cal_mkt_value()
            self.data_calculation.cal_margin_info()
        else:
            print("Portfolio is empty")
        pass