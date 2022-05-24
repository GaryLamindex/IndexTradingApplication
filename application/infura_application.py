import web3
from web3 import Web3

infura_url = "https://mainnet.infura.io/v3/11a8af4ff6d847cca2564187af91843e"
web_3 = Web3(Web3.HTTPProvider(infura_url))

wallet_address = "0xC352B534e8b987e036A93539Fd6897F53488e56a"
transaction_address = "0x35926ff7d42d7e9accfffe2928d8e82ea8943190ffc134f09db06732a56bae04"
print("isConnected:", web_3.isConnected())

balance = web_3.eth.get_balance(wallet_address)
web_3.toJSON(balance)
print("balance:", balance)
print("balance:", web_3.fromWei(balance, "ether"))

transaction = web_3.eth.getTransaction(transaction_address)
print("transaction:", transaction)

