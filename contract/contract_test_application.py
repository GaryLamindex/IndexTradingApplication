from web3 import Web3

web3 = Web3(Web3.HTTPProvider('http://127.0.0.1:7545'))
print(web3.isConnected())

abi = json.loads('ABI JSON CODE HERE')
address = '0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82'

contract = web3.eth.contract(address=address, abi=abi)
user = ["hello", 10];
HelloWorld.transact().addUser(user);