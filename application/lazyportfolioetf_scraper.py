import json
import os
import pathlib
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import requests


class lazyportfolioetf_engine:
    ticker_name_path = None
    ticker_data_path = None

    def __init__(self):
        self.portfolio_path = str(
            pathlib.Path(__file__).parent.parent.parent.resolve()) + "/etf_list/portfolio.csv"
        self.etf_list_path = str(pathlib.Path(__file__)
                                 .parent.parent.parent.resolve()) + '/etf_list'

    def get_portfolio(self):
        file_exists = os.path.exists(self.portfolio_path)
        if file_exists:
            return
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
        df = pd.DataFrame(columns=['Weight', 'Ticker', 'Strategy Name'])
        for x in range(0, len(links)):
            soup1 = BeautifulSoup(requests.get(links[x]).text, "lxml")
            table = soup1.find('table',
                               class_='w3-table table-padding-small w3-small font-family-arial table-valign-middle')
            for row in table.tbody.find_all('tr'):
                columns = row.find_all('td')
                if columns != []:
                    weight = columns[0].b.contents[0]
                    ticker = columns[2].b.contents[0]
                    strategy_name = soup1.title.string
                    strategy_name = strategy_name.replace(': ETF allocation and returns', '')
                    df = pd.concat([df, pd.DataFrame.from_records(
                        [{'Weight': weight, 'Ticker': ticker, 'Strategy Name': strategy_name}])])
        df.to_csv(self.portfolio_path, index=False)

def main():
    pass
    # url = "http://www.lazyportfolioetf.com/allocation/"
    # page = urlopen(url)
    # html_bytes = page.read()
    # html = html_bytes.decode("utf-8")
    # soup = BeautifulSoup(html, "lxml")
    # portfolio_link = False
    # links = list()
    # for link in soup.find_all('a', href=True):
    #     if link['href'] == "http://www.lazyportfolioetf.com/allocation/10-year-treasury/":
    #         portfolio_link = True
    #     if link[
    #         'href'] == "https://twitter.com/intent/tweet?text=Lazy%20Portfolios:%20ETF%20Allocation&url=http://www.lazyportfolioetf.com/allocation":
    #         portfolio_link = False
    #     if portfolio_link:
    #         links.append(link['href'])
    # links = list(dict.fromkeys(links))
    # df = pd.DataFrame(columns=['Weight', 'Ticker', 'Strategy Name'])
    # scraper_path = str(
    #     pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + "/Rainy Drop/etf_list/scraper.csv"
    # for x in range(0, len(links)):
    #     soup1 = BeautifulSoup(requests.get(links[x]).text, "lxml")
    #     table = soup1.find('table',
    #                        class_='w3-table table-padding-small w3-small font-family-arial table-valign-middle')
    #     for row in table.tbody.find_all('tr'):
    #         columns = row.find_all('td')
    #         if columns != []:
    #             weight = columns[0].b.contents[0]
    #             ticker = columns[2].b.contents[0]
    #             strategy_name = soup1.title.string
    #             strategy_name = strategy_name.replace(': ETF allocation and returns', '')
    #             df = pd.concat([df, pd.DataFrame.from_records(
    #                 [{'Weight': weight, 'Ticker': ticker, 'Strategy Name': strategy_name}])])
    # df.to_csv(scraper_path, index=False)
    # df = pd.DataFrame()
    # array = list()
    # miss = list()
    # exist = True
    # ticker_data_path = str(
    #     pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + "/Rainy Drop/ticker_data/one_min"
    # for x in range(0, len(links)):
    #     soup1 = BeautifulSoup(requests.get(links[x]).text, "lxml")
    #     table = soup1.find('table',
    #                        class_='w3-table table-padding-small w3-small font-family-arial table-valign-middle')
    #     for row in table.tbody.find_all('tr'):
    #         columns = row.find_all('td')
    #         if columns != []:
    #             weight = str(columns[0].b.contents[0])
    #             weight = weight.replace("%", "")
    #             ticker = str(columns[2].b.contents[0])
    #             strategy_name = soup1.title.string
    #             strategy_name = strategy_name.replace(': ETF allocation and returns', '')
    #             file_exist = f"{ticker}.csv" in os.listdir(ticker_data_path)
    #             if not file_exist:
    #                 exist = False
    #                 miss.append(ticker)
    #             res = {ticker: weight}
    #             res = json.dumps(res)
    #             array.append(res)
    #     if not exist:
    #         a = "False"
    #     else:
    #         a = "True"
    #     df = pd.concat(
    #         [df, pd.DataFrame.from_records([{'Strategy Name': strategy_name, 'Json': array.copy(), 'Data?': a,'File Missed': miss.copy()}])])
    #     array.clear()
    #     miss.clear()
    #     exist = True
    # df.to_excel("/Users/percychui/Downloads/scraper2.xlsx", index=False)


if __name__ == "__main__":
    main()
