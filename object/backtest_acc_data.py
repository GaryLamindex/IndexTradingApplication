import json
import pathlib


class backtest_acc_data(object):
    user_id = 0
    trading_funds = {}
    mkt_value = {}
    margin_acc = {}
    deposit_withdraw_cash_record = {}
    portfolio = []
    margin_info = []
    stock_transaction_record = []
    cashflow_record= []
    acc_data_json_file_path = ""
    table_name = ""
    def __init__(self, user_id, strategy_name, table_name, spec_str):
        self.user_id = user_id
        self.strategy_name = strategy_name
        self.table_name = table_name
        self.portfolio = []
        self.stock_transaction_record = []
        self.deposit_withdraw_cash_record = []
        # the above 4 are for the entire account
        self.acc_data = {"AccountCode": 0, "Currency": "HKD", "ExchangeRate": 0}
        self.margin_acc = {'FullInitMarginReq': 0, 'FullMaintMarginReq': 0}
        self.trading_funds = {'AvailableFunds': 0, "ExcessLiquidity": 0, "BuyingPower": 0, "Leverage": 0,
                              "EquityWithLoanValue": 0}
        self.mkt_value = {"TotalCashValue": 0, "NetDividend": 0, "NetLiquidation": 0, "UnrealizedPnL": 0,
                          "RealizedPnL": 0, "GrossPositionValue": 0}

        _spy = {'ticker': 'SPY', 'initMarginReq': 0.09, 'maintMarginReq': 0.1}
        _qqq = {'ticker': 'QQQ', 'initMarginReq': 0.1, 'maintMarginReq': 0.11}
        _govt = {'ticker': 'GOVT', 'initMarginReq': 0.09, 'maintMarginReq': 0.1}
        _shv = {'ticker': 'SHV', 'initMarginReq': 0.09, 'maintMarginReq': 0.1}
        self.margin_info = [_spy, _qqq, _govt, _shv]
        self.acc_data_json_file_path = str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()) + f"/user_id_{self.user_id}/backtest/acc_data/{self.table_name}/{spec_str}.json"
    # for stocks !
    def update_portfolio_item(self, ticker, position, marketPrice, averageCost, marketValue, realizedPNL, unrealizedPNL, initMarginReq, maintMarginReq, costBasis):
        updating_portfolio_dict = {'ticker': ticker, 'position': position, "marketPrice": marketPrice, 'averageCost': averageCost, "marketValue": marketValue,
                           "realizedPNL": realizedPNL, "unrealizedPNL": unrealizedPNL, 'initMarginReq': initMarginReq, 'maintMarginReq': maintMarginReq,
                           "costBasis": costBasis}
        if len(self.portfolio) == 0:
            self.portfolio.append(updating_portfolio_dict)
        else:
            for item in self.portfolio:
                if item.get("ticker") == ticker:
                    item.update((k,v) for k,v in updating_portfolio_dict.items() if v is not None)
                    break

    def append_stock_transaction_record(self, ticker, timestamp, transaction_type, position_purchase, ticker_open_price, transaction_amount,
                                         position):
        transaction_type_dict = {0:"Buy", 1:"Sell"}
        record = {'ticker': ticker, "timestamp":timestamp, 'transaction_type': transaction_type_dict.get(transaction_type),
                                         "position_purchase": position_purchase, 'ticker_open_price': ticker_open_price, 'transaction_amount': transaction_amount,
                                         'position': position}
        self.stock_transaction_record.append(record)

    def append_cashflow_record(self, timestamp, transaction_type, amount):
        transaction_type_dict = {0: "Deposit", 1: "Withdraw"}
        record = {'timestamp': timestamp, 'transaction_type': transaction_type_dict.get(transaction_type), 'amount': amount}
        self.cashflow_record.append(record)

    def update_acc_data(self, AccountCode, Currency, ExchangeRate):
        updating_acc_data_dict = {"AccountCode": AccountCode, "Currency": Currency, "ExchangeRate": ExchangeRate}
        self.acc_data.update((k,v) for k,v in updating_acc_data_dict.items() if v is not None)

    def update_margin_acc(self, FullInitMarginReq, FullMaintMarginReq):
        updating_margin_acc_dict = {'FullInitMarginReq': FullInitMarginReq, 'FullMaintMarginReq': FullMaintMarginReq}
        self.margin_acc.update((k,v) for k,v in updating_margin_acc_dict.items() if v is not None)

    def update_trading_funds(self, AvailableFunds, ExcessLiquidity, BuyingPower, Leverage, EquityWithLoanValue):
        updating_trading_funds_dict = {'AvailableFunds': AvailableFunds, 'ExcessLiquidity': ExcessLiquidity, 'BuyingPower': BuyingPower,
                                       'Leverage': Leverage, 'EquityWithLoanValue': EquityWithLoanValue}
        self.trading_funds.update((k,v) for k,v in updating_trading_funds_dict.items() if v is not None)

    def update_mkt_value(self, TotalCashValue, NetDividend, NetLiquidation, UnrealizedPnL, RealizedPnL, GrossPositionValue):
        updating_mkt_value_dict = {"TotalCashValue": TotalCashValue, "NetDividend": NetDividend, "NetLiquidation": NetLiquidation, "UnrealizedPnL": UnrealizedPnL,
                          "RealizedPnL": RealizedPnL, "GrossPositionValue": GrossPositionValue}
        self.mkt_value.update( (k,v) for k,v in updating_mkt_value_dict.items() if v is not None)

    def check_if_ticker_exist_in_portfolio(self, ticker):
        tickers = [d['ticker'] for d in self.portfolio]
        if ticker in tickers:
            return True
        else:
            return False

    def get_portfolio_ticker_item(self, ticker):
        ticker_item = next((item for item in self.portfolio if item['ticker'] == ticker), None)
        return ticker_item

    def update_portfolio_ticker_item(self, ticker_item):
        ticker = ticker_item.get("ticker")
        for item in self.portfolio:
            if item.get("ticker") == ticker:
                item.update((k,v) for k,v in ticker_item.items() if v is not None)
                break

    def get_margin_info_ticker_item(self, ticker):
        margin_info_item = next((item for item in self.margin_info if item['ticker'] == ticker), None)
        return margin_info_item

    def return_acc_data(self):
        portfolio_json = json.dumps(self.portfolio)
        stock_transaction_record_json = json.dumps(self.stock_transaction_record)
        deposit_withdraw_cash_record_json = json.dumps(self.deposit_withdraw_cash_record)

        acc_data_json = json.dumps(self.acc_data)
        mkt_value_json = json.dumps(self.mkt_value)
        margin_acc_json = json.dumps(self.margin_acc)
        trading_funds_json = json.dumps(self.trading_funds)

        data_dict = {"portfolio":portfolio_json, "stock_transaction_record":stock_transaction_record_json, "deposit_withdraw_cash_record":deposit_withdraw_cash_record_json,
                "acc_data":acc_data_json, "mkt_value":mkt_value_json, "margin_acc":margin_acc_json, "trading_funds":trading_funds_json
                }

        return data_dict

    def read_acc_data(self):
        acc_data_json_file = pathlib.Path(self.acc_data_json_file_path)
        if acc_data_json_file.is_file():
            data_dict = {}
            with open(self.acc_data_json_file_path, 'r') as f:
                data_dict = json.load(f)

            portfolio_json = data_dict.get("portfolio")
            stock_transaction_record_json = data_dict.get("stock_transaction_record")
            deposit_withdraw_cash_record_json = data_dict.get("deposit_withdraw_cash_record")

            acc_data_json = data_dict.get("acc_data")
            mkt_value_json = data_dict.get("mkt_value")
            margin_acc_json = data_dict.get("margin_acc")
            trading_funds_json = data_dict.get("trading_funds")

            self.portfolio = json.loads(portfolio_json)
            self.stock_transaction_record = json.loads(stock_transaction_record_json)
            self.deposit_withdraw_cash_record = json.loads(deposit_withdraw_cash_record_json)

            self.acc_data = json.loads(acc_data_json)
            self.mkt_value = json.loads(mkt_value_json)
            self.margin_acc = json.loads(margin_acc_json)
            self.trading_funds = json.loads(trading_funds_json)

        pass