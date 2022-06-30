from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import pandas as pd


def main():
    url = "http://www.lazyportfolioetf.com/allocation/"
    page = urlopen(url)
    html_bytes = page.read()
    html = html_bytes.decode("utf-8")
    soup = BeautifulSoup(html, "lxml")
    find = False
    a = list()
    for link in soup.find_all('a', href=True):
        if link['href'] == "http://www.lazyportfolioetf.com/allocation/10-year-treasury/":
            find = True
        if link[
            'href'] == "https://twitter.com/intent/tweet?text=Lazy%20Portfolios:%20ETF%20Allocation&url=http://www.lazyportfolioetf.com/allocation":
            find = False
        if find:
            a.append(link['href'])
    a = list(dict.fromkeys(a))
    weight = list()
    ticker_name = list()
    for x in range(0, len(a)):
        tmp = re.findall(r'<b>(.*?)</b></a></td>', urlopen(a[x]).read().decode("utf-8"))
        ticker_name.append(tmp)
        tmp1 = (re.findall(r'<td class="w3-center" style=""><b>(.*?)</b></td>', urlopen(a[x]).read().decode("utf-8")))
        if len(tmp) != len(tmp1):
            tmp1 = tmp1[:-2]
        weight.append(tmp1.copy())
    weight = [x for xs in weight for x in xs]
    ticker_name = [x for xs in ticker_name for x in xs]
    df = pd.DataFrame()
    df["weight"] = weight
    df["ticker"] = ticker_name
    df.to_excel("/Users/percychui/Downloads/scraper.xlsx", index=False)


if __name__ == "__main__":
    main()
