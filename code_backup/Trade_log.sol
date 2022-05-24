pragma solidity ^0.8.0;
import "https://github.com/OpenZeppelin/openzeppelin-contracts/blob/master/contracts/token/ERC1155/ERC1155.sol"

contract Trade_log{
    uint total_transaction_qty;
    string _latest_trade_log;
    string timestamp;

    string trade_log = 10;


    function set_trade_log(string memory _ticker, string memory _avg_price, string memory _transact_qty) external {
        _latest_trade_log = string(bytes.concat(
                bytes("{"),
                bytes("ticker:"),
                bytes(_ticker),
                bytes(",avg_price:"),
                bytes(_avg_price),
                bytes(",transact_qty:"),
                bytes(_transact_qty),
                bytes(",timestamp:"),
                bytes(uint2str(block.timestamp)),
                bytes("}")
            ));

        if (total_transaction_qty == 0){
            trade_log = _latest_trade_log;
        }else{
            trade_log = string(bytes.concat(bytes(trade_log), ",", bytes(_latest_trade_log)));
        }
        total_transaction_qty += 1;
    }

    function get_trade_log() external view returns(string memory){
        return trade_log;
    }

    function init() external{
        trade_log = "";
        total_transaction_qty = 0;
    }

    function uint2str(uint _i) internal pure returns (string memory) {
        if (_i == 0) {
            return "0";
        }
        uint j = _i;
        uint len;
        while (j != 0) {
            len++;
            j /= 10;
        }
        bytes memory bstr = new bytes(len);
        uint k = len;
        while (_i != 0) {
            k = k-1;
            uint8 temp = (48 + uint8(_i - _i / 10 * 10));
            bytes1 b1 = bytes1(temp);
            bstr[k] = b1;
            _i /= 10;
        }
        return string(bstr);
    }
}
