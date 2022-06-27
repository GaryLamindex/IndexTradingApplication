import json
import pathlib

from web3 import Web3, HTTPProvider

blockchian_address = "http://127.0.0.1:7545"
web3 = Web3(HTTPProvider(blockchian_address))
web3.eth.defaultAccount = web3.eth.accounts[0]
contract_folder_path = str(pathlib.Path(__file__).parent.parent.resolve()) + "/contract"


compiled_contract_path = contract_folder_path+"/build/contracts/RebalanceMarginWifMaxDrawdownControl.json"
deployed_contract_address = "0x4901eCF0F59b95b56DFb22a214581714D4560C08"

with open(compiled_contract_path) as file:
    contract_json = json.load(file)
    contract_abi = contract_json['abi']

contract = web3.eth.contract(address = deployed_contract_address, abi=contract_abi)
# _tickerData = [[("tickerName", "SPY"),("bidPrice",1), ("last",2)],[("tickerName", "QQQ"),("bidPrice",3), ("last",4)]]
_tickerData = [["SPY",1,2],["QQQ",1,4]]
_timeStamp = 123
_portfolioData = [
    ("AccountCode",0),
    ("Currency","USD"),
    ("ExchangeRate","0.78"),
    ("FullInitMarginReq","0.9"),
    ("FullMainMarginReq","0.8"),
    ("AvailableFunds","100"),
    ("ExcessLiquidity","200"),
    ("BuyingPower","300"),
    ("Leverage","3"),
    ("EquityWithLoanValue","300"),
    ("TotalCashValue","400"),
    ("NetDividend","500"),
    ("NetLiquidation","600"),
    ("UnrealizedPnL","700"),
    ("RealizedPnL","800"),
    ("GrossPositionValue","900")
]

_portfolioHoldings = [
    [("tickerName","SPY"),
     ("averageCost",0),
        ("costBasis",1),
        ("initMarginReq",2),
        ("maintMarginReq",3),
        ("marketPrice","4"),
        ("marketValue",5),
        ("position",6),
        ("realizedPNL",7),
        ("unrealizedPNL",8)],

[                       ("tickerName","QQQ"),
                       ("averageCost",0),
                       ("costBasis",1),
                       ("initMarginReq",2),
                       ("maintMarginReq",3),
                       ("marketPrice","4"),
                       ("marketValue",5),
                       ("position",6),
                       ("realizedPNL",7),
                       ("unrealizedPNL",8)]
                      ]

result = contract.functions.run(_tickerData, _timeStamp, _portfolioData, _portfolioHoldings)