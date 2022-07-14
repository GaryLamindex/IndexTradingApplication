import json

from web3 import Web3, HTTPProvider

class web3_algo:
    def __init__(self, contract_address, contract_abi, account_address):
        self.contract_address = contract_address
        self.contract_abi = contract_abi
        self.account_address = account_address
        self.web3 = Web3(HTTPProvider('http://localhost:8545'))
        self.web3.eth.defaultAccount = self.account_address
        self.web3.eth.defaultBlock = "latest"
        self.contract = self.web3.eth.contract(address=self.contract_address, abi=self.contract_abi)


    def inputRealTimeTickerData(self, ticker, price, last, timestamp):
        self.contract.functions.inputRealTimeTickerData(ticker, price, last, timestamp).transact()

    def inputMarginAccount(self, timestamp, fullInitMarginReq, fullMainMarginReq):
        self.contract.functions.inputMarginAccount(timestamp, fullInitMarginReq, fullMainMarginReq).transact()

    def inputTradingFunds(self, timestamp, availableFunds, excessLiquidity, buyingPower, leverage, equityWithLoanValue):
        self.contract.functions.inputTradingFunds(timestamp, availableFunds, excessLiquidity, buyingPower, leverage, equityWithLoanValue).transact()

    def inputMktValue(self, timestamp, totalCashValue, netDividend, netLiquidation, unrealizedPnL, realizedPnL, grossPositionValue):
        self.contract.functions.inputMktValue(timestamp, totalCashValue, netDividend, netLiquidation, unrealizedPnL, realizedPnL, grossPositionValue).transact()

    def updatePortfolioHoldingsData(self, timestamp, tickerName, position, marketPrice, averageCost, marketValue, realizedPNL, unrealizedPNL, initMarginReq, maintMarginReq, costBasis):
        self.contract.functions.updatePortfolioHoldingsData(timestamp, tickerName, position, marketPrice, averageCost, marketValue, realizedPNL, unrealizedPNL, initMarginReq, maintMarginReq, costBasis).transact()

    def getTickerActionMsg(self, ticker, timestamp):
        msg = self.contract.functions.getTickerActionMsg(ticker, timestamp).call()
        if msg[0] == "":
            return None
        return msg


    def getPortfolioHolding(self, ticker, timestamp):
        msg = self.contract.functions.getPortfolioHolding(ticker, timestamp).call()
        if msg[0] == "":
            return None
        return msg

    def getRealTimeTickerData(self, ticker, timestamp):
        msg = self.contract.functions.getRealTimeTickerData(ticker, timestamp).call()
        if msg[0] == "":
            return None
        return msg

    def getMarginAccount(self, timestamp):
        msg = self.contract.functions.getMarginAccount(timestamp).call()
        if msg[0] == "":
            return None
        return msg

    def getTradingFunds(self, timestamp):
        msg = self.contract.functions.getTradingFunds(timestamp).call()
        if msg[0] == "":
            return None
        return msg
    def getMktValue(self, timestamp):
        msg = self.contract.functions.getMktValue(timestamp).call()
        if msg[0] == "":
            return None
        return msg

    def getPortfolioHolding(self, tickerName, timestamp):
        msg = self.contract.functions.getPortfolioHolding(tickerName, timestamp).call()
        if msg[0] == "":
            return None
        return msg

    def reinitializeContractState(self):
        self.contract.functions.reinitializeContractState().call()

    def inputLoopWithTimestamps(self, loop, timestamp):
        self.contract.functions.inputLoopWithTimestamps(loop, timestamp).call()

def contract_test(algo):

    def contract_test(self):
        print(algo.reinitializeContractState())

        print(algo.getTickerActionMsg("AAPL", 1564656465))
        print(algo.getPortfolioHolding("AAPL", 1564656465))
        print(algo.getRealTimeTickerData("AAPL", 1564656465))
        print(algo.getRealTimeTickerData("GOOG", 1564656465))
        print(algo.getRealTimeTickerData("AAPL", 1564656465))
        print(algo.getMarginAccount(1564656465))
        print(algo.getTradingFunds(1564656465))
        print(algo.getMktValue(1564656465))
        print(algo.getPortfolioHolding("AAPL", 1564656465))

        algo.inputLoopWithTimestamps(1, 1564656465)
        algo.inputRealTimeTickerData("AAPL", 100, 100, 1564656465)
        algo.inputMarginAccount(1564656465, 100, 100)
        algo.inputTradingFunds(1564656465, 100, 100, 100, 100, 100)
        algo.inputMktValue(1564656465, 100, 100, 100, 100, 100, 100)
        algo.updatePortfolioHoldingsData(1564656465, "AAPL", 100, 100, 100, 100, 100, 100, 100, 100, 100)

        print(algo.getTickerActionMsg("AAPL", 1564656465))
        print(algo.getPortfolioHolding("AAPL", 1564656465))
        print(algo.getRealTimeTickerData("AAPL", 1564656465))
        print(algo.getRealTimeTickerData("GOOG", 1564656465))
        print(algo.getRealTimeTickerData("AAPL", 1564656465))
        print(algo.getMarginAccount(1564656465))
        print(algo.getTradingFunds(1564656465))
        print(algo.getMktValue(1564656465))
        print(algo.getPortfolioHolding("AAPL", 1564656465))

def contract_run_test(algo):
    algo.reinitializeContractState()
    timestamp = 1564656465

    algo.inputRealTimeTickerData("AAPL", 100, 100, timestamp)
    algo.inputRealTimeTickerData("GOOG", 100, 100, timestamp)

    algo.inputMarginAccount(timestamp, 100, 100)
    algo.inputTradingFunds(timestamp, 100, 100, 100, 100, 100)
    algo.inputMktValue(timestamp, 100, 100, 100, 100, 100, 100)


if __name__ == '__main__':
    with open("RebalanceMarginWifMaxDrawdownControl.json") as f:
        info_json = json.load(f)
    abi = info_json["abi"]

    address = "0x383B32AeC5074c740815aC8d15EfABaE4518E90D"
    default_account = "0xE357eaFcE4d472c344e638E72ce55dE8C6b62992"
    algo = web3_algo(address, abi, default_account)
    contract_test(algo)



