import pandas as pd
from object.action_data import BinanceAction, BinanceActionsTuple


class momentum_strategy:
    def __init__(self, portfolio_agent):
        self.portfolio_agent = portfolio_agent
        self.portfolio = self.portfolio_agent.acc_data.portfolio

    # it should return proper format of tuples in a list for self.pending_actions in backtest.py
    def run(self, price_dict, periods_pct_change_dict, timestamp):
        df = pd.DataFrame.from_dict(periods_pct_change_dict, orient='index', columns=['pct_change'])
        df.dropna(inplace=True)

        actions_tup = None

        pending_action_list = []

        if (df['pct_change'] > 0).any():  # at least one can buy
            df_temp = df.dropna(subset='pct_change')
            if df_temp.shape[0] <= 1:
                return []
            n_positive = df.loc[df['pct_change'] > 0].shape[0]
            n_max = max(n_positive, 30)
            df_largest = df_temp['pct_change'].nlargest(n_max)

            for p in self.portfolio:
                if p['ticker'] not in df_largest.index:
                    actions_tup = BinanceActionsTuple(timestamp + 86400, BinanceAction.CLOSE_POSITION,
                                                      {'ticker': p['ticker']})

            for ticker, pct_change in df_largest.iteritems():
                total_num = df_largest.shape[0]
                cash = self.portfolio_agent.get_overview().get('funding') / total_num
                cur_price = price_dict[ticker]
                qty_to_buy = int(cash / cur_price)

                # only for integral qty_to_buy
                if qty_to_buy == 0:
                    continue

                # metadata isn't available at this stage
                # will be accommodated later
                action = BinanceAction.BUY_MKT_ORDER
                actions_tup = BinanceActionsTuple(timestamp + 86400, BinanceAction.BUY_MKT_ORDER,
                                                  {'ticker': ticker, 'position_purchase': qty_to_buy})
        else:  # all non-positive, empty all position and won't buy
            actions_tup = BinanceActionsTuple(timestamp + 86400, BinanceAction.CLOSE_ALL, None)

        if actions_tup is not None:
            pending_action_list.append(actions_tup)

        return pending_action_list
