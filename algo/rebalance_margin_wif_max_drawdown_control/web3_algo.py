import json

from web3 import Web3, HTTPProvider

BIGNUMBER = 1000000000000000000000000

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
        price = int(price * BIGNUMBER)
        last = int(last * BIGNUMBER)
        self.contract.functions.inputRealTimeTickerData(ticker, price, last, timestamp).transact()

    def inputMarginAccount(self, timestamp, fullInitMarginReq, fullMainMarginReq):
        fullInitMarginReq = int(fullInitMarginReq * BIGNUMBER)
        fullMainMarginReq = int(fullMainMarginReq * BIGNUMBER)
        self.contract.functions.inputMarginAccount(timestamp, fullInitMarginReq, fullMainMarginReq).transact()


    def inputTradingFunds(self, timestamp, availableFunds, excessLiquidity, buyingPower, leverage, equityWithLoanValue):
        availableFunds = int(availableFunds * BIGNUMBER)
        excessLiquidity = int(excessLiquidity * BIGNUMBER)
        buyingPower = int(buyingPower * BIGNUMBER)
        leverage = int(leverage * BIGNUMBER)
        equityWithLoanValue = int(equityWithLoanValue * BIGNUMBER)

        self.contract.functions.inputTradingFunds(timestamp, availableFunds, excessLiquidity, buyingPower, leverage, equityWithLoanValue).transact()

    def inputMktValue(self, timestamp, totalCashValue, netDividend, netLiquidation, unrealizedPnL, realizedPnL, grossPositionValue):
        totalCashValue = int(totalCashValue * BIGNUMBER)
        netDividend = int(netDividend * BIGNUMBER)
        netLiquidation = int(netLiquidation * BIGNUMBER)
        unrealizedPnL = int(unrealizedPnL * BIGNUMBER)
        realizedPnL = int(realizedPnL * BIGNUMBER)
        grossPositionValue = int(grossPositionValue * BIGNUMBER)
        self.contract.functions.inputMktValue(timestamp, totalCashValue, netDividend, netLiquidation, unrealizedPnL, realizedPnL, grossPositionValue).transact()

    def updatePortfolioHoldingsData(self, timestamp, tickerName, position, marketPrice, averageCost, marketValue, realizedPNL, unrealizedPNL, initMarginReq, maintMarginReq, costBasis):
        position = int(position * BIGNUMBER)
        marketPrice = int(marketPrice * BIGNUMBER)
        averageCost = int(averageCost * BIGNUMBER)
        marketValue = int(marketValue * BIGNUMBER)
        realizedPNL = int(realizedPNL * BIGNUMBER)
        unrealizedPNL = int(unrealizedPNL * BIGNUMBER)
        initMarginReq = int(initMarginReq * BIGNUMBER)
        maintMarginReq = int(maintMarginReq * BIGNUMBER)
        costBasis = int(costBasis * BIGNUMBER)
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
        self.contract.functions.inputLoopWithTimestamps(loop, timestamp).transact()

    def run(self, timestamp):
        self.contract.functions.run(timestamp).call()

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

    algo.inputRealTimeTickerData("QQQ", 65.11, 65.11, timestamp)

    algo.inputMarginAccount(timestamp, 0, 0)
    algo.inputTradingFunds(timestamp, 1000000, 1000000, 6666666, 0, 1000000)
    algo.inputMktValue(timestamp, 1000000, 0, 1000000, 0, 0, 0)

    print(algo.getTickerActionMsg("QQQ", timestamp))
    print(algo.getPortfolioHolding("QQQ", timestamp))
    print(algo.getRealTimeTickerData("QQQ", timestamp))
    print(algo.getMarginAccount(timestamp))
    print(algo.getTradingFunds(timestamp))
    print(algo.getMktValue(timestamp))

    algo.run(timestamp)
    print(algo.getTickerActionMsg("QQQ", timestamp))



if __name__ == '__main__':
    with open("RebalanceMarginWifMaxDrawdownControl.json") as f:
        info_json = json.load(f)
    abi = info_json["abi"]

    address = "0xAcE05bFAD89aBD18b62b12924758ae82348F3AF6"
    default_account = "0xE357eaFcE4d472c344e638E72ce55dE8C6b62992"
    algo = web3_algo(address, abi, default_account)
    contract_run_test(algo)

#let algo00 = await RebalanceMarginWifMaxDrawdownControl.deployed()
#algo00.run(1564656465)


