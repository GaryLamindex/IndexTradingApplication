import pandas as pd
import crypto_algo


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
            df_largest = df_temp['pct_change'].nlargest(1, 'all')
            for ticker, pct_change in df_largest.iteritems():
                # TODO: need to change
                if self.portfolio_agent.acc_data.check_if_ticker_exist_in_portfolio(ticker):
                    action = crypto_algo.momentum_strategy_crypto.backtest.Action.CLOSE_POSITION
                    actions_tup = crypto_algo.momentum_strategy_crypto.backtest.ActionsTuple(timestamp + 86400, action,
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
                action = crypto_algo.momentum_strategy_crypto.backtest.Action.BUY_MKT_ORDER
                actions_tup = crypto_algo.momentum_strategy_crypto.backtest.ActionsTuple(timestamp + 86400, action,
                                                                                         {'ticker': ticker,
                                                                                          'position_purchase':
                                                                                              qty_to_buy})
        else:  # all non-positive, empty all position and won't buy
            action = crypto_algo.momentum_strategy_crypto.backtest.Action.CLOSE_ALL
            actions_tup = crypto_algo.momentum_strategy_crypto.backtest.ActionsTuple(timestamp + 86400, action, None)

        if actions_tup is not None:
            pending_action_list.append(actions_tup)

        return pending_action_list
