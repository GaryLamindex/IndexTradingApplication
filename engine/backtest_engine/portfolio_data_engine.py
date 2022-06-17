from datetime import datetime


class backtest_portfolio_data_engine(object):
    acc_data = None

    def __init__(self, backtest_acc_data, tickers):
        self.acc_data = backtest_acc_data
        self.init_stock_position(tickers)

    def deposit_cash(self, amount, timestamp):
        mkt_value = self.acc_data.mkt_value
        TotalCashValue = mkt_value.get("TotalCashValue")

        TotalCashValue += amount
        self.acc_data.update_mkt_value(TotalCashValue, None, None, None, None, None)
        self.update_acc_data()

        self.acc_data.append_cashflow_record(timestamp, amount, 0)

        print('Cash deposit :', amount)

    def withdraw_cash(self, amount, timestamp):
        mkt_value = self.acc_data.mkt_value
        TotalCashValue = mkt_value.get("TotalCashValue")

        TotalCashValue -= amount

        self.acc_data.update_mkt_value(self, TotalCashValue, None, None, None, None, None)
        self.update_acc_data()

        self.acc_data.append_cashflow_record(timestamp, amount, 1)

        print('Cash withdraw :', amount)

    def init_stock_position(self, tickers):
        for ticker in tickers:
            self.acc_data.update_portfolio_item(ticker, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    def update_stock_price_and_portfolio_data(self, stock_data_dict):

        portfolio = self.acc_data.portfolio
        tickers = [r['ticker'] for r in portfolio]
        if len(tickers) > 0:
            #print("Updating Portfolio and Stock Data")
            for ticker in tickers:
                if stock_data_dict.get(ticker) != None:
                    # Update price
                    ticker_item = self.acc_data.get_portfolio_ticker_item(ticker)
                    marketPrice = stock_data_dict[ticker]['last']  # Retrieve stock data of date
                    #print("updated price:", marketPrice, "; updated stock:", ticker)
                    position = ticker_item.get('position')
                    cost_basis = ticker_item.get('costBasis')
                    marketValue = marketPrice * position
                    unrealizedPNL = marketValue - cost_basis
                    self.acc_data.update_portfolio_item(ticker, position, marketPrice, None, marketValue, None,
                                                        unrealizedPNL, None, None, None)

            self.update_acc_data()
        else:
            #print("Portfolio is empty")

        #print('update_portfolio_data')
            pass

    def update_acc_data(self):
        mkt_value = self.acc_data.mkt_value
        portfolio = self.acc_data.portfolio

        GrossPositionValue = 0
        total_costBasis = 0

        # Get assets
        TotalCashValue = mkt_value.get("TotalCashValue")
        if (len(portfolio) != 0):
            GrossPositionValue = sum([r['marketValue'] for r in portfolio])
            total_costBasis = sum([r['costBasis'] for r in portfolio])

        # Calculation
        NetLiquidation = TotalCashValue + GrossPositionValue
        UnrealizedPnL = GrossPositionValue - total_costBasis
        RealizedPnL = sum([r['realizedPNL'] for r in portfolio])

        # Update the DB
        self.acc_data.update_mkt_value(TotalCashValue, None, NetLiquidation, UnrealizedPnL, RealizedPnL,
                                       GrossPositionValue)

        # update margin info
        TotalCashValue = mkt_value.get("TotalCashValue")
        GrossPositionValue = mkt_value.get("GrossPositionValue")
        tickers = [r['ticker'] for r in portfolio]
        NetLiquidation = mkt_value.get("NetLiquidation")

        for ticker in tickers:
            ticker_item = self.acc_data.get_portfolio_ticker_item(ticker)
            costBasis = ticker_item.get("costBasis")
            try:
                ticker_init_margin = costBasis * self.acc_data.get_margin_info_ticker_item(ticker).get("initMarginReq")
                ticker_mnt_margin = costBasis * self.acc_data.get_margin_info_ticker_item(ticker).get("maintMarginReq")
            except AttributeError:
                ticker_init_margin = costBasis
                ticker_mnt_margin = costBasis
            self.acc_data.update_portfolio_item(ticker, None, None, None, None, None, None, ticker_init_margin,
                                                ticker_mnt_margin, None)

        FullInitMarginReq = sum([r['initMarginReq'] for r in portfolio])
        FullMaintMarginReq = sum([r['maintMarginReq'] for r in portfolio])
        EquityWithLoanValue = TotalCashValue + GrossPositionValue
        if EquityWithLoanValue - FullInitMarginReq > 0:
            AvailableFunds = EquityWithLoanValue - FullInitMarginReq
        else:
            AvailableFunds = 0
        BuyingPower = AvailableFunds * 20 / 3
        ExcessLiquidity = EquityWithLoanValue - FullMaintMarginReq
        # print("NetLiquidation:", NetLiquidation, "; GrossPositionValue:", GrossPositionValue, "; TotalCashValue:",
        #       TotalCashValue, "; FullInitMarginReq:", FullInitMarginReq, "; FullMaintMarginReq:", FullMaintMarginReq,
        #       "; EquityWithLoanValue:", EquityWithLoanValue, "; AvailableFunds:", AvailableFunds, ";BuyingPower:",
        #       BuyingPower, "; ExcessLiquidity:", ExcessLiquidity)
        if NetLiquidation == 0:
            Leverage = 0
        else:
            Leverage = GrossPositionValue / NetLiquidation

        self.acc_data.update_margin_acc(FullInitMarginReq, FullMaintMarginReq)
        self.acc_data.update_trading_funds(AvailableFunds, ExcessLiquidity, BuyingPower, Leverage,
                                           EquityWithLoanValue)

    # return a dictionary of data of the account
    def get_account_snapshot(self):
        # account_snapshot = {"date":datetime.now(),"timestamp":datetime.now().timestamp()}
        account_snapshot = {}
        account_snapshot.update(self.acc_data.trading_funds)
        account_snapshot.update(self.acc_data.mkt_value)
        # account_snapshot.update({"portfolio":self.acc_data.portfolio})
        for stock_item in self.acc_data.portfolio:
            temp_list = stock_item.copy()  # get a copy of the stock_item dictionary
            ticker = temp_list["ticker"]
            del temp_list['ticker']  # get rid of the "ticker" column, since the csv does NOT contain this attribute
            res = {f"{str(key)}_{ticker}": val for key, val in temp_list.items()}
            account_snapshot.update(res)

        return account_snapshot

    def get_portfolio(self):
        return self.acc_data.portfolio
