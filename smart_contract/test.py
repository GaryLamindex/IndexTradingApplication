import json
from web3 import Web3, HTTPProvider

blockchain_address = "HTTP://127.0.0.1:7545"
web3 = Web3(HTTPProvider(blockchain_address))
web3.eth.defaultAccount = web3.eth.accounts[0]

compiled_contract_path = "build/contracts/Trade_log.json"
deployed_contract_address = "0x80df0FD2b8dC0b1c5043340B413D4fb0D18201C7"

with open(compiled_contract_path) as file:
    contract_json = json.load(file)
    contract_abi = contract_json["abi"]

contract = web3.eth.contract(address=deployed_contract_address, abi=contract_abi)
print("a")
message = contract.functions.init().call()
print(message)

_ticker = "SPY"
_avg_price = 30
_transact_qty = 1

while(True):
    print("b")
    result = contract.functions.set_trade_log(_ticker, str(_avg_price), str(_transact_qty)).transact()
    print(result)
    print("c")
    message = contract.functions.get_trade_log().call()
    print(message)
    _avg_price += 1
    _transact_qty += 1