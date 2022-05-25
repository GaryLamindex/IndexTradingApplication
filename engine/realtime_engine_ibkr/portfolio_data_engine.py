import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.parent.resolve()))

from ib_insync import *
from object.ibkr_acc_data import ibkr_acc_data
from failure_handler import connection_handler, connect_tws
import datetime as dt

class ibkr_portfolio_data_engine:

    acc_data = None
    ib_instance = None

    def __init__(self, ibkr_acc_data,ib_instance):
        self.acc_data = ibkr_acc_data
        self.ib_instance = ib_instance
    
    @connection_handler
    def get_acc_raw_data_from_ib(self):
        connect_tws(self.ib_instance)
        # fetch the account snapshot
        self.ib_instance.sleep(0)
        raw_balance = self.ib_instance.accountValues() # a snapshot of the positions (account details)
        return raw_balance

    @connection_handler
    def get_portfolio_raw_data_from_ib(self):
        connect_tws(self.ib_instance)
        self.ib_instance.sleep(0)
        pofolio = self.ib_instance.portfolio()
        return pofolio

    def update_stock_price_and_portfolio_data(self, *args):
        print()
        protfolio_raw_data = self.get_portfolio_raw_data_from_ib()
        acc_raw_data = self.get_acc_raw_data_from_ib()

        # update "portfolio"
        for stock in protfolio_raw_data:
            ticker = stock[0].symbol
            position = stock[1]
            marketPrice = stock[2]
            marketValue = stock[3]
            averageCost = stock[4]
            unrealizedPNL = stock[5]
            realizedPNL = stock[6]
            initMarginReq = None # to be modified
            maintMarginReq = None # to be modified
            costBasis = None # to be modified
            self.acc_data.update_portfolio_item(ticker,position,marketPrice,averageCost,marketValue,realizedPNL,unrealizedPNL,initMarginReq,maintMarginReq,costBasis)
        
        # update "acc_data","margin_acc","trading_funds","mkt_value"
        # fetch all the useful data inside a loop first
        currency = "USD" # the base currency
        for item in acc_raw_data:
            if (item.tag == 'AccountCode'):
                AccountCode = item.value
            if (item.tag == 'ExchangeRate' and item.currency == 'USD'): # the exchange rate of USD int terms of base (HKD)
                ExchangeRate = float(item.value)
        
        for item in acc_raw_data:
            if (item.tag == 'FullInitMarginReq-S'):
                FullInitMarginReq = float(item.value) / float(ExchangeRate)
            if (item.tag == 'FullMaintMarginReq-S'):
                FullMaintMarginReq = float(item.value) / float(ExchangeRate)
            if (item.tag == 'FullAvailableFunds-S'):
                AvailableFunds = float(item.value) / float(ExchangeRate)
            if (item.tag == 'ExcessLiquidity-S'):
                ExcessLiquidity = float(item.value) / float(ExchangeRate)
            if (item.tag == 'BuyingPower'):
                BuyingPower = float(item.value) / float(ExchangeRate)
            if (item.tag == 'Leverage-S'):
                Leverage = float(item.value) / float(ExchangeRate)
            if (item.tag == 'EquityWithLoanValue-S'):
                EquityWithLoanValue = float(item.value) / float(ExchangeRate)
            if (item.tag == 'TotalCashValue-S'):
                TotalCashValue = float(item.value) / float(ExchangeRate)
            if (item.tag == 'NetDividend' and item.currency == currency):
                NetDividend = float(item.value)
            if (item.tag == 'NetLiquidationByCurrency' and item.currency == currency):
                NetLiquidation = float(item.value)
            if (item.tag == 'UnrealizedPnL' and item.currency == currency):
                UnrealizedPnL = float(item.value)
            if (item.tag == 'RealizedPnL' and item.currency == currency):
                RealizedPnL = float(item.value)
            if (item.tag == 'GrossPositionValue-S'):
                GrossPositionValue = float(item.value) / float(ExchangeRate)

        # call the update function 
        self.acc_data.update_acc_data(AccountCode,currency,ExchangeRate)
        self.acc_data.update_margin_acc(FullInitMarginReq,FullMaintMarginReq)
        self.acc_data.update_trading_funds(AvailableFunds,ExcessLiquidity,BuyingPower,Leverage,EquityWithLoanValue)
        self.acc_data.update_mkt_value(TotalCashValue,NetDividend,NetLiquidation,UnrealizedPnL,RealizedPnL,GrossPositionValue)

    # return a dictionary of data of the account
    def get_account_snapshot(self):
        account_snapshot = {"date":dt.datetime.now(),"timestamp":dt.datetime.now().timestamp()}
        account_snapshot.update(self.acc_data.trading_funds)
        account_snapshot.update(self.acc_data.mkt_value)
        for stock_item in self.acc_data.portfolio:
            temp_list = stock_item.copy() # get a copy of the stock_item dictionary
            ticker = stock_item["ticker"]
            # res = {f"{ticker} {str(key)}": val for key, val in stock_item.items()}
            del temp_list['ticker']  # get rid of the "ticker" column, since the csv does NOT contain this attribute
            account_snapshot.update(temp_list)
        return account_snapshot

def main():
    ib = IB()
    ib.connect('127.0.0.1',7497,clientId=1)
    acc_obj = ibkr_acc_data(0,"","","")
    engine = ibkr_portfolio_data_engine(acc_obj,ib)
    engine.update_stock_price_and_portfolio_data()
    print(engine.acc_data.portfolio)
    print(engine.acc_data.acc_data)
    print(engine.acc_data.margin_acc)
    print(engine.acc_data.trading_funds)
    print(engine.acc_data.mkt_value)
    print(engine.acc_data.portfolio)

if __name__ == "__main__":
    main()

"""
Portofolio data:
[PortfolioItem(contract=Stock(conId=320227571, symbol='QQQ', right='0', primaryExchange='NASDAQ', 
currency='USD', localSymbol='QQQ', tradingClass='NMS'), position=122.0, marketPrice=380.2999878, marketValue=46396.6, 
averageCost=381.54606555, unrealizedPNL=-152.02, realizedPNL=0.0, account='DU2785296')]

Account data:
[AccountValue(account='DU2785296', tag='AccountCode', value='DU2785296', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='AccountOrGroup', value='DU2785296', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='AccountOrGroup', value='DU2785296', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='AccountOrGroup', value='DU2785296', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='AccountReady', value='true', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='AccountType', value='INDIVIDUAL', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='AccruedCash', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='AccruedCash', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='AccruedCash', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='AccruedCash-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='AccruedCash-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='AccruedDividend', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='AccruedDividend-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='AccruedDividend-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='AvailableFunds', value='7740941.70', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='AvailableFunds-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='AvailableFunds-S', value='7740941.70', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='Billable', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='Billable-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='Billable-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='BuyingPower', value='51606277.99', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='CashBalance', value='7421167.7441', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='CashBalance', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='CashBalance', value='953023.20', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='ColumnPrio-P', value='12', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='ColumnPrio-S', value='3', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='CorporateBondValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='CorporateBondValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='CorporateBondValue', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='Cryptocurrency', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='Cryptocurrency', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='Cryptocurrency', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='Currency', value='BASE', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='Currency', value='HKD', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='Currency', value='USD', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='Cushion', value='0.99515', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='EquityWithLoanValue', value='7782456.89', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='EquityWithLoanValue-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='EquityWithLoanValue-S', value='7782456.89', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='ExcessLiquidity', value='7744715.81', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='ExcessLiquidity-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='ExcessLiquidity-S', value='7744715.81', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='ExchangeRate', value='1.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='ExchangeRate', value='1.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='ExchangeRate', value='7.786325', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullAvailableFunds', value='7740941.70', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullAvailableFunds-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullAvailableFunds-S', value='7740941.70', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullExcessLiquidity', value='7744715.81', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullExcessLiquidity-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullExcessLiquidity-S', value='7744715.81', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullInitMarginReq', value='41515.19', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullInitMarginReq-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullInitMarginReq-S', value='41515.19', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullMaintMarginReq', value='37741.09', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullMaintMarginReq-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FullMaintMarginReq-S', value='37741.09', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FundValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='FundValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FundValue', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='FutureOptionValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='FutureOptionValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FutureOptionValue', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='FuturesPNL', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='FuturesPNL', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FuturesPNL', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='FxCashBalance', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='FxCashBalance', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='FxCashBalance', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='GrossPositionValue', value='361289.15', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='GrossPositionValue-S', value='361289.15', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='Guarantee', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='Guarantee-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='Guarantee-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='IndianStockHaircut', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='IndianStockHaircut-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='IndianStockHaircut-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='InitMarginReq', value='41515.19', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='InitMarginReq-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='InitMarginReq-S', value='41515.19', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='IssuerOptionValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='IssuerOptionValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='IssuerOptionValue', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='Leverage-S', value='0.05', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadAvailableFunds', value='7740941.70', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadAvailableFunds-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadAvailableFunds-S', value='7740941.70', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadExcessLiquidity', value='7744715.81', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadExcessLiquidity-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadExcessLiquidity-S', value='7744715.81', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadInitMarginReq', value='41515.19', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadInitMarginReq-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadInitMarginReq-S', value='41515.19', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadMaintMarginReq', value='37741.09', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadMaintMarginReq-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadMaintMarginReq-S', value='37741.09', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='LookAheadNextChange', value='1642429800', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='MaintMarginReq', value='37741.09', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='MaintMarginReq-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='MaintMarginReq-S', value='37741.09', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='MoneyMarketFundValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='MoneyMarketFundValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='MoneyMarketFundValue', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='MutualFundValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='MutualFundValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='MutualFundValue', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='NLVAndMarginInReview', value='false', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='NetDividend', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='NetDividend', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='NetDividend', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='NetLiquidation', value='7782456.89', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='NetLiquidation-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='NetLiquidation-S', value='7782456.89', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='NetLiquidationByCurrency', value='7782456.8925', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='NetLiquidationByCurrency', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='NetLiquidationByCurrency', value='999419.7985', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='NetLiquidationUncertainty', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='OptionMarketValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='OptionMarketValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='OptionMarketValue', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='PASharesValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PASharesValue-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PASharesValue-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PhysicalCertificateValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PhysicalCertificateValue-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PhysicalCertificateValue-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PostExpirationExcess', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PostExpirationExcess-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PostExpirationExcess-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PostExpirationMargin', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PostExpirationMargin-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='PostExpirationMargin-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='RealCurrency', value='BASE', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='RealCurrency', value='HKD', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='RealCurrency', value='USD', currency='USD', modelCode=''),
AccountValue(account='DU2785296', tag='RealizedPnL', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='RealizedPnL', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='RealizedPnL', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='SegmentTitle-P', value='Crypto at Paxos', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='SegmentTitle-S', value='Securities', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='StockMarketValue', value='361289.15', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='StockMarketValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='StockMarketValue', value='46396.60', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='TBillValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='TBillValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='TBillValue', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='TBondValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='TBondValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='TBondValue', value='0.00', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='TotalCashBalance', value='7421167.7441', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='TotalCashBalance', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='TotalCashBalance', value='953023.20', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='TotalCashValue', value='7421167.74', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='TotalCashValue-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='TotalCashValue-S', value='7421167.74', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='TotalDebitCardPendingCharges', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='TotalDebitCardPendingCharges-P', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='TotalDebitCardPendingCharges-S', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='TradingType-S', value='STKNOPT', currency='', modelCode=''), 
AccountValue(account='DU2785296', tag='UnrealizedPnL', value='-1183.79', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='UnrealizedPnL', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='UnrealizedPnL', value='-152.02', currency='USD', modelCode=''), 
AccountValue(account='DU2785296', tag='WarrantValue', value='0.00', currency='BASE', modelCode=''), 
AccountValue(account='DU2785296', tag='WarrantValue', value='0.00', currency='HKD', modelCode=''), 
AccountValue(account='DU2785296', tag='WarrantValue', value='0.00', currency='USD', modelCode='')]
"""