import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from algo.random_forest.indicator import Indicator
from object.action_data import IBAction, IBActionsTuple

class RandomForest:
    def __init__(self, trade_agent, portfolio_agent):
        self.account_snapshot = {}
        self.portfolio = []
        self.trade_agent = trade_agent
        self.portfolio_agent = portfolio_agent
        self.last_exec_datetime_obj = None
        self.portfolio = self.account_snapshot.get("portfolio")
        self.total_market_value = self.account_snapshot.get("NetLiquidation")
        self.buy = ""
        self.optimal_weight = {}
        self.action_msgs = []

    def run(self, price_dict, all_indice_df, timestamp):
        # remove price_dict param if possible
        if not self.trade_agent.market_opened():
            return

        self.portfolio_agent.update_stock_price_and_portfolio_data(price_dict)
        self.account_snapshot = self.portfolio_agent.get_account_snapshot()
        self.portfolio = self.portfolio_agent.get_portfolio()
        self.total_market_value = self.account_snapshot.get("NetLiquidation")

        return_dict = {}
        for ticker in all_indice_df.iteritems():


            indicator = Indicator(index)
            indicator.get_samples()
            X, y = indicator.dataset.drop("Return", axis=1), indicator.dataset["Return"]
            regr = RandomForestRegressor(max_features=1 / 3,
                                         min_samples_leaf=0.005,
                                         random_state=23571113,
                                         max_samples=0.7)
            regr.fit(X, y)
            return_dict[ticker] = regr.predict()[0]
        positive_return_ticker = [ticker for ticker, ret in return_dict.items if ret > 0]
        total_return = sum([ret for ret in return_dict.values() if ret > 0])
        self.optimal_weight = {ticker: return_dict[ticker] / total_return for ticker in positive_return_ticker}

        self.action_msgs = []

        for ticker_data in self.portfolio:
            ticker_name = ticker_data.get("ticker")
            ticker_pos = ticker_data.get("position")
            if ticker_name in all_indice_df.columns:
                price = all_indice_df[ticker_name][-1]
                print("Weight:", self.optimal_weight[ticker_name])
                target_pos = int(self.optimal_weight[ticker_name] * self.total_market_value / price)
                print(f"{ticker_name} Current Position: {ticker_pos}; Target Position: {target_pos}; Market Value: {self.total_market_value}")
                pos_change = target_pos - ticker_pos
                if pos_change < 0:
                    action_msg = IBActionsTuple(timestamp, IBAction.SELL_MKT_ORDER,
                                                {'ticker': ticker_name, 'position_sell': -pos_change})
                    self.action_msgs.append(action_msg)
                elif pos_change > 0:
                    action_msg = IBActionsTuple(timestamp, IBAction.BUY_MKT_ORDER,
                                                {'ticker': ticker_name, 'position_purchase': pos_change})
                    self.action_msgs.append(action_msg)

        return self.action_msgs.copy()