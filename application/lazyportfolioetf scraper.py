import json
import os
import pathlib
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import requests


def main():
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
    # df = pd.DataFrame(columns=['Weight', 'Ticker', 'Strategy Name'])
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
    #             df = pd.concat([df, pd.DataFrame.from_records([{'Weight': weight, 'Ticker': ticker, 'Strategy Name': strategy_name}])])
    # df.to_excel("/Users/percychui/Downloads/scraper.xlsx", index=False)
    df = pd.DataFrame()
    array = list()
    miss = list()
    exist = True
    ticker_data_path = str(
        pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + "/Rainy Drop/ticker_data/one_min"
    for x in range(0, len(links)):
        soup1 = BeautifulSoup(requests.get(links[x]).text, "lxml")
        table = soup1.find('table',
                           class_='w3-table table-padding-small w3-small font-family-arial table-valign-middle')
        for row in table.tbody.find_all('tr'):
            columns = row.find_all('td')
            if columns != []:
                weight = columns[0].b.contents[0]
                weight = float(weight.replace("%", ""))
                ticker = columns[2].b.contents[0]
                strategy_name = soup1.title.string
                strategy_name = strategy_name.replace(': ETF allocation and returns', '')
                file_exist = f"{ticker}.csv" in os.listdir(ticker_data_path)
                if not file_exist:
                    exist = False
                    miss.append(ticker)
                res = {ticker: weight}
                array.append(res)
        if not exist:
            a = "False"
        else:
            a = "True"
        json_str = json.dumps(array)
        df = pd.concat(
            [df, pd.DataFrame.from_records([{'Strategy Name': strategy_name, 'Json': json_str, 'Data?': a,'File Missed': miss.copy()}])])
        array.clear()
        miss.clear()
        exist = True
    df.to_excel("/Users/percychui/Downloads/scraper2.xlsx", index=False)


if __name__ == "__main__":
    main()
