import pandas as pd
from object.action_data import BinanceAction, BinanceActionsTuple


class momentum_strategy:
    def __init__(self, portfolio_agent):
        self.portfolio_agent = portfolio_agent

    # it should return proper format of tuples in a list for self.pending_actions in backtest.py
    def run(self, price_dict, periods_pct_change_dict, high_dict, timestamp):
        portfolio = self.portfolio_agent.acc_data.portfolio
        df = pd.DataFrame.from_dict(periods_pct_change_dict, orient='index', columns=['pct_change'])
        df.dropna(inplace=True)

        pending_action_list = []
        n_positive = df.loc[df['pct_change'] > 0].shape[0]
        n_threshold = 30

        if n_positive >= n_threshold:
            df_temp = df.dropna(subset='pct_change')
            if df_temp.shape[0] <= 1:
                return []
            df_largest = df_temp['pct_change'].nlargest(n_threshold)
            for ticker, _ in df_largest.iteritems():
                if price_dict[ticker] != high_dict[ticker]:
                    df_largest.drop(inplace=True, index=ticker)

            for p in portfolio:
                if p['ticker'] not in df_largest.index:
                    pending_action_list.append(BinanceActionsTuple(timestamp + 86400, BinanceAction.CLOSE_POSITION,
                                                                   {'ticker': p['ticker']}))

            for ticker, pct_change in df_largest.iteritems():
                pending_action_list.append(BinanceActionsTuple(timestamp + 86400, BinanceAction.BUY_MKT_ORDER,
                                                               {'ticker': ticker,
                                                                'avg_holding': n_threshold}))
        else:  # all non-positive, empty all position and won't buy
            pending_action_list.append(BinanceActionsTuple(timestamp + 86400, BinanceAction.CLOSE_ALL, None))

        return pending_action_list
