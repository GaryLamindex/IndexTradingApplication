import pandas as pd

import algo.momentum_strategy.backtest


class momentum_strategy:
    def __init__(self, portfolio_agent):
        self.portfolio_agent = portfolio_agent
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("NetLiquidation")

    # it should return proper format of tuples in a list for self.pending_actions in backtest.py
    def run(self, price_dict, periods_pct_change_dict, timestamp):
        df = pd.DataFrame.from_dict(periods_pct_change_dict, orient='index', columns=['pct_change'])

        pending_action_list = []

        if (df['pct_change'] > 0).any():  # at least one can buy
            df_largest = df['pct_change'].nlargest(1, 'all')
            for ticker, pct_change in df_largest.iteritems():
                buying_power = self.portfolio_agent.get_account_snapshot().get('BuyingPower')
                cur_price = price_dict[ticker]
                qty_to_buy = int(buying_power / cur_price * 0.9)

                # metadata isn't available at this stage
                # will be accommodated later
                action = algo.momentum_strategy.backtest.Action.BUY_MKT_ORDER
                pending_action_list.append((timestamp + 86400, action,
                                            {'ticker': ticker,
                                             'position_purchase': qty_to_buy}))
        else:  # all non-positive, empty all position and won't buy
            action = algo.momentum_strategy.backtest.Action.CLOSE_ALL
            pending_action_list.append((timestamp + 86400, action, None))

        return pending_action_list



