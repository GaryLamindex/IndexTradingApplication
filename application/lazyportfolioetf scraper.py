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
        if link['href'] == "https://twitter.com/intent/tweet?text=Lazy%20Portfolios:%20ETF%20Allocation&url=http://www.lazyportfolioetf.com/allocation":
            find = False
        if find:
            a.append(link['href'])
    a = list(dict.fromkeys(a))
    weight = list()
    ticker_name = list()
    for x in range(0, len(a)):
        tmp = (re.findall(r'<td class="w3-center" style=""><b>(.*?)</b></td>', urlopen(a[x]).read().decode("utf-8")))
        tmp = tmp[:-2]
        weight.append(tmp.copy())
        ticker_name.append(re.findall(r'<b>(.*?)</b></a></td>', urlopen(a[x]).read().decode("utf-8")))
    weight = [x for xs in weight for x in xs]
    ticker_name = [x for xs in ticker_name for x in xs]
    df = pd.DataFrame()
    df["weight"] = weight
    df["ticker"] = ticker_name
    print(df)
    df.to_excel("/Users/percychui/Downloads/scraper.xlsx")
    # url2 = a[-1]
    # page2 = urlopen(url2)
    # html_bytes2 = page2.read()
    # html2 = html_bytes2.decode("utf-8")
    # with open("/Users/percychui/Downloads/html3.txt", "w") as f:
    #     print(html2, file=f)
    # soup2 = BeautifulSoup(html, "html.parser")
    # weight = re.findall(r'<td class="w3-center" style=""><b>(.*?)</b></td>', html2)
    # weight = weight[:-2]
    # p = list()
    # p.append(weight.copy())
    # print(p)
    # ticker_name = re.findall(r'<b>(.*?)</b></a></td>', html2)
    # print(ticker_name)



if __name__ == "__main__":
    main()
