from tinydb import TinyDB, Query, where


class data_calculation_engine(object):
    path = ""
    db_path = ""
    acc_data = None
    margin_acc = None
    mkt_value = None
    portfolio = None
    trading_funds = None
    margin_info = None
    cash_record = None
    stock_transaction_record = None
    deposit_withdraw_cash_record = None

    def __init__(self, path):
        self.path = path
        self.db_path = path + "/db"
        self.acc_data = TinyDB(self.db_path + '/acc_data.json')
        self.margin_acc = TinyDB(self.db_path + '/margin_acc.json')
        self.mkt_value = TinyDB(self.db_path + '/mkt_value.json')
        self.portfolio = TinyDB(self.db_path + '/portfolio.json')
        self.trading_funds = TinyDB(self.db_path + '/trading_funds.json')
        self.margin_info = TinyDB(self.db_path + '/margin_info.json')
        self.cash_record = TinyDB(self.db_path + '/cash_record.json')
        self.stock_transaction_record = TinyDB(self.db_path + '/stock_transaction_record.json')
        self.deposit_withdraw_cash_record = TinyDB(self.db_path + "/deposit_withdraw_cash_record.json")

    def deposit_cash(self, amount, date):
        print(self.db_path + "/mkt_value.json")
        mkt_value = TinyDB(self.db_path + "/mkt_value.json", create_dirs = True)

        _mkt_value_dict = mkt_value.all()[0]
        TotalCashValue = _mkt_value_dict.get("TotalCashValue")

        TotalCashValue += amount
        type = "deposit"

        mkt_value.update({'TotalCashValue': TotalCashValue}, Query().TotalCashValue.exists())
        self.cal_mkt_value()
        self.cal_margin_info()
        self.record(amount, str(date), type)

        print('Cash deposit :', amount)

        msg = {'ticker': "None", 'action': 'deposit cash', 'deposited amount': amount}
        return msg

    def withdraw_cash(self, amount, date):
        mkt_value = TinyDB(self.db_path + "/db/mkt_value.json")

        _mkt_value_dict = mkt_value.all()[0]
        cash = _mkt_value_dict.get("TotalCashValue")

        cash -= amount
        type = "withdraw"

        mkt_value.update({'cash': cash}, Query().a.exists())
        self.cal_mkt_value()
        self.cal_margin_info()
        self.record(amount, date, type)

        print('Cash withdraw :', amount)

        msg = {'ticker': "None", 'action': 'withdraw cash', 'withdraw amount': amount}
        return msg

    def record(self, amount, date, type):
        cash_record_db = TinyDB(self.db_path + "/deposit_withdraw_cash_record.json")
        cash_record_db.insert({'date':date,'deposit withdraw amount':amount, "type":type})

    def cal_mkt_value(self):
        #Connect to DB
        mkt_value = TinyDB(self.db_path+'/mkt_value.json')
        portfolio = TinyDB(self.db_path+'/portfolio.json')
        GrossPositionValue = 0
        total_costBasis = 0

        #Get assets
        cash = mkt_value.all()[0].get("TotalCashValue")
        if(portfolio.search(Query().marketValue.exists())):
            GrossPositionValue = sum([r['marketValue'] for r in portfolio])
            total_costBasis = sum([r['costBasis'] for r in portfolio])

        #Calculation
        net_liquid_value = cash + GrossPositionValue
        unrlz_pl = GrossPositionValue - total_costBasis
        rlz_pl = sum([r['realizedPNL'] for r in portfolio])

        #Update the DB
        mkt_value.update({'NetLiquidation': net_liquid_value}, Query().NetLiquidation.exists())
        mkt_value.update({'UnrealizedPnL': unrlz_pl}, Query().UnrealizedPnL.exists())
        mkt_value.update({'RealizedPnL': rlz_pl}, Query().RealizedPnL.exists())
        mkt_value.update({'GrossPositionValue': GrossPositionValue}, Query().GrossPositionValue.exists())

        return mkt_value

    def cal_margin_info(self):

        margin_acc = TinyDB(self.db_path+'/margin_acc.json')
        mkt_value = TinyDB(self.db_path + '/mkt_value.json')
        portfolio = TinyDB(self.db_path + '/portfolio.json')
        trading_funds = TinyDB(self.db_path + '/trading_funds.json')
        margin_info = TinyDB(self.db_path + '/margin_info.json')

        TotalCashValue = mkt_value.all()[0].get("TotalCashValue")
        GrossPositionValue = mkt_value.all()[0].get("GrossPositionValue")
        tickers = [r['ticker'] for r in portfolio]
        NetLiquidation = mkt_value.all()[0].get("NetLiquidation")

        for ticker in tickers:
            costBasis = portfolio.get(Query().ticker == ticker).get("costBasis")
            initMarginReq = margin_info.get(Query().ticker == ticker).get("initMarginReq")
            maintMarginReq = margin_info.get(Query().ticker == ticker).get("maintMarginReq")

            ticker_init_margin = costBasis * initMarginReq
            ticker_mnt_margin = costBasis * maintMarginReq

            portfolio.update_multiple([({'initMarginReq': ticker_init_margin}, where('ticker') == ticker),
                                       ({'maintMarginReq': ticker_mnt_margin}, where('ticker') == ticker)
                                       ])

        FullInitMarginReq = sum([r['initMarginReq'] for r in portfolio])
        FullMaintMarginReq = sum([r['maintMarginReq'] for r in portfolio])
        EquityWithLoanValue = TotalCashValue + GrossPositionValue
        if EquityWithLoanValue - FullInitMarginReq >0:
            AvailableFunds = EquityWithLoanValue - FullInitMarginReq
        else:
            AvailableFunds = 0
        BuyingPower = AvailableFunds * 20/3
        ExcessLiquidity = EquityWithLoanValue - FullMaintMarginReq
        print("NetLiquidation:",NetLiquidation,"; GrossPositionValue:", GrossPositionValue, "; TotalCashValue:", TotalCashValue, "; FullInitMarginReq:",FullInitMarginReq,"; FullMaintMarginReq:",FullMaintMarginReq,"; EquityWithLoanValue:", EquityWithLoanValue, "; AvailableFunds:", AvailableFunds, ";BuyingPower:", BuyingPower, "; ExcessLiquidity:", ExcessLiquidity)
        if NetLiquidation == 0:
            Leverage = 0
        else:
            Leverage = GrossPositionValue / NetLiquidation
        margin_acc.update_multiple([({"FullInitMarginReq":FullInitMarginReq}, Query().FullInitMarginReq.exists()),
                                       ({"FullMaintMarginReq":FullMaintMarginReq}, Query().FullMaintMarginReq.exists())
                                       ])
        trading_funds.update_multiple([({"EquityWithLoanValue":EquityWithLoanValue}, Query().EquityWithLoanValue.exists()),
                                       ({"AvailableFunds":AvailableFunds}, Query().AvailableFunds.exists()),
                                       ({"BuyingPower":BuyingPower}, Query().BuyingPower.exists()),
                                       ({"ExcessLiquidity":ExcessLiquidity}, Query().ExcessLiquidity.exists()),
                                       ({"Leverage":Leverage}, Query().Leverage.exists())
                                       ])


    def update_stock_price_and_portfolio_data(self, stock_data_dict):

        portfolio = TinyDB(self.db_path + '/portfolio.json')
        tickers = [r['ticker'] for r in portfolio]
        if len(tickers) > 0:
            print("Updating Portfolio and Stock Data")
            for ticker in tickers:
                #Update price
                marketPrice = stock_data_dict.get(ticker) #Retrieve stock data of date
                print("updated price:",marketPrice,"; updated stock:",ticker)
                position = portfolio.get(Query().ticker == ticker).get('position')
                cost_basis = portfolio.get(Query().ticker == ticker).get('costBasis')
                marketValue = marketPrice * position
                unrealizedPNL = marketValue - cost_basis

                portfolio.update_multiple([({"marketPrice": marketPrice},  where('ticker') == ticker),
                                            ({"marketValue": marketValue}, where('ticker') == ticker),
                                            ({"unrealizedPNL": unrealizedPNL}, where('ticker') == ticker)
                                            ])
            data_cal_engine = data_calculation_engine(self.path)
            data_cal_engine.cal_mkt_value()
            data_cal_engine.cal_margin_info()
        else :
            print("Portfolio is empty")

        print('update_portfolio_data')
        pass