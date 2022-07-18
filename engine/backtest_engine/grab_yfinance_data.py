import yfinance as yf
import os.path
import pathlib
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import requests


class yfinance_data_engine:
    ticker_name_path = None
    ticker_data_path = None

    def __init__(self):
        self.ticker_data_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.resolve()) + '/ticker_data/one_day'
        self.ticker_name_path = str(pathlib.Path(__file__)
                                    .parent.parent.parent.parent.resolve()) + '/etf_list'

    def get_yfinance_max_historical_data(self, ticker):
        btc = yf.Ticker(ticker)
        hist = btc.history(period='max')
        return hist

    def get_ticker_name(self):
        url = "http://www.lazyportfolioetf.com/allocation/"
        page = urlopen(url)
        html_bytes = page.read()
        html = html_bytes.decode("utf-8")
        soup = BeautifulSoup(html, "lxml")
        portfolio_link = False
        links = list()
        for link in soup.find_all('a', href=True):
            if link['href'] == "http://www.lazyportfolioetf.com/allocation/10-year-treasury/":
                portfolio_link = True
            if link[
                'href'] == "https://twitter.com/intent/tweet?text=Lazy%20Portfolios:%20ETF%20Allocation&url=http://www.lazyportfolioetf.com/allocation":
                portfolio_link = False
            if portfolio_link:
                links.append(link['href'])
        links = list(dict.fromkeys(links))
        df = pd.DataFrame(columns=['Ticker'])
        for x in range(0, len(links)):
            soup1 = BeautifulSoup(requests.get(links[x]).text, "lxml")
            table = soup1.find('table',
                               class_='w3-table table-padding-small w3-small font-family-arial table-valign-middle')
            for row in table.tbody.find_all('tr'):
                columns = row.find_all('td')
                if columns != []:
                    ticker = columns[2].b.contents[0]
                    df = pd.concat([df, pd.DataFrame.from_records([{'Ticker': ticker}])])
        df = df.drop_duplicates()
        df.to_csv(f"{self.ticker_name_path}/ticker_name.csv", index=False)


def main():
    engine = yfinance_data_engine()
    if not os.path.isdir(engine.ticker_data_path):
        os.makedirs(engine.ticker_data_path)
    if not os.path.isdir(engine.ticker_name_path):
        os.makedirs(engine.ticker_name_path)
    file_exists = os.path.exists(f"{engine.ticker_name_path}/ticker_name.csv")
    # if not file_exists:
    #     engine.get_ticker_name()
    ticker_name = pd.read_csv(f"{engine.ticker_name_path}/ticker_name.csv")
    for ticker in ticker_name['Ticker']:
        df = engine.get_yfinance_max_historical_data(ticker)
        index_list = df.index.tolist()
        timestamp = list()
        for x in range(len(index_list)):
            timestamp.append(int(index_list[x].timestamp()))
        df['timestamp'] = timestamp
        df = df.rename(columns={'Open': 'open'})
        df.to_csv(f"{engine.ticker_data_path}/{ticker}.csv", index=True, header=True)
        print(f"Successfully download {ticker}.csv")


if __name__ == '__main__':
    main()