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
    df = pd.DataFrame(columns=['Weight', 'Ticker'])
    for x in range(0, len(links)):
        soup1 = BeautifulSoup(requests.get(links[x]).text, "lxml")
        table = soup1.find('table',
                           class_='w3-table table-padding-small w3-small font-family-arial table-valign-middle')
        for row in table.tbody.find_all('tr'):
            columns = row.find_all('td')
            if columns != []:
                weight = columns[0].b.contents[0]
                ticker = columns[2].b.contents[0]
                df = pd.concat([df, pd.DataFrame.from_records([{'Weight': weight, 'Ticker': ticker}])])
    df.to_excel("/Users/percychui/Downloads/scraper.xlsx", index=False)
    """
    dataframe with only two columns, but there will exist same ticker with different weights as there are many 
    investment themes
    """
    df2 = pd.DataFrame(columns=['Weight', 'Ticker', 'ETF_name', 'Investment theme'])
    for x in range(0, len(links)):
        soup2 = BeautifulSoup(requests.get(links[x]).text, "lxml")
        table = soup2.find('table',
                           class_='w3-table table-padding-small w3-small font-family-arial table-valign-middle')
        for row in table.tbody.find_all('tr'):
            columns = row.find_all('td')
            if columns != []:
                weight = columns[0].b.contents[0]
                ticker = columns[2].b.contents[0]
                etf_name = columns[3].a.contents[0]
                invest = columns[4].text.strip()
                df2 = pd.concat([df2, pd.DataFrame.from_records(
                    [{'Weight': weight, 'Ticker': ticker, 'ETF_name': etf_name, 'Investment theme': invest}])])
    df2.to_excel("/Users/percychui/Downloads/scraper2.xlsx", index=False)
    """
    dataframe with whole table
    """

if __name__ == "__main__":
    main()
