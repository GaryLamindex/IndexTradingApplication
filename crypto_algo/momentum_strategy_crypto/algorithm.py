import pandas as pd
from object.action_data import BinanceAction, BinanceActionsTuple


class momentum_strategy:
    def __init__(self, portfolio_agent):
        self.portfolio_agent = portfolio_agent
        self.portfolio = self.portfolio_agent.acc_data.portfolio

    # it should return proper format of tuples in a list for self.pending_actions in backtest.py
    def run(self, price_dict, periods_pct_change_dict, timestamp):
        df = pd.DataFrame.from_dict(periods_pct_change_dict, orient='index', columns=['pct_change'])
        actions_tup = None

        pending_action_list = []

        if (df['pct_change'] > 0).any():  # at least one can buy
            df_temp = df.dropna(subset='pct_change')
            if df_temp.shape[0] <= 1:
                return []
            df_largest = df_temp['pct_change'].nlargest(1)

            for ticker, pct_change in df_largest.iteritems():
                # TODO: buggy: should loop thru portfolio instead
                if self.portfolio_agent.acc_data.check_if_ticker_exist_in_portfolio(ticker):
                    actions_tup = BinanceActionsTuple(timestamp + 86400, BinanceAction.CLOSE_POSITION,
                                                      {'ticker': ticker})

            for ticker, pct_change in df_largest.iteritems():
                cash = self.portfolio_agent.get_overview().get('funding')
                cur_price = price_dict[ticker]
                qty_to_buy = int(cash / cur_price * 0.99)

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
