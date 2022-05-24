from ib_insync import *
from IPython.display import display, clear_output
import pandas as pd

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)
contracts = [Forex(pair) for pair in ('EURUSD', 'USDJPY', 'GBPUSD', 'USDCHF', 'USDCAD', 'AUDUSD')]
ib.qualifyContracts(*contracts)
df = pd.DataFrame(
    index=[c.pair() for c in contracts],
    columns=['bidSize', 'bid', 'ask', 'askSize', 'high', 'low', 'close'])

for contract in contracts:
    ib.reqMktData(contract,'',False,False)

def onPendingTickers(tickers):
    for t in tickers:
        df.loc[t.contract.pair()] = (
            t.bidSize, t.bid, t.ask, t.askSize, t.high, t.low, t.close)
        clear_output(wait=True)
    print(df)

while True:
    ib.pendingTickersEvent += onPendingTickers
    ib.sleep(5)
    ib.pendingTickersEvent -= onPendingTickers