pragma solidity ^0.8.0;
import "./ChampOwnership.sol";

/// @title A facet of KittyCore that manages Kitty siring, gestation, and birth.
/// @author Axiom Zen (https://www.axiomzen.co)
/// @dev See the KittyCore contract documentation to understand how the various contract facets are arranged.
contract ChampLogging is ChampOwnership {
    event LogTrade(address owner);
    uint256 public autoLogFee = 2 * 1e15;
    uint256 public activeChamps = 0;
    function setAutoLogFee(uint256 val) external onlyCOO {
        autoLogFee = val;
    }

    function isActive(uint256 _champId) public view returns (bool){
        return champTraders[_champId].totalTransactionQty != 0;
    }

    function setlogTrade(uint _champId, string memory _ticker, string memory _avgPrice, string memory _transactQty) external whenNotPaused{

        require(_isTrader(msg.sender, _champId));

        // Grab a reference to the matron in storage.
        ChampTrader storage champ = champTraders[_champId];

        // Check that the matron is a valid cat.
        require(champ.birthTime != 0);
        string memory _latestTradeLog = string(bytes.concat(
                bytes("{"),
                bytes("ticker:"),
                bytes(_ticker),
                bytes(",avg_price:"),
                bytes(_avgPrice),
                bytes(",transact_qty:"),
                bytes(_transactQty),
                bytes(",timestamp:"),
                bytes(uint2str(block.timestamp)),
                bytes("}")
            ));
        bool _is_active = isActive(_champId);
        if(_is_active){
            champ.tradeLog = string(bytes.concat(bytes(champ.tradeLog), ",", bytes(_latestTradeLog)));
        }else{
            champ.tradeLog = _latestTradeLog;
            activeChamps +=1;
        }
        champ.totalTransactionQty += 1;

        payable(msg.sender).transfer(autoLogFee);
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


    function get_trade_log(uint256 _champId) external view returns(string memory){
        require(_isTraderorOwner(msg.sender, _champId));
        // Grab a reference to the matron in storage.
        ChampTrader storage champ = champTraders[_champId];
        return champ.tradeLog;
    }

    function init(uint256 _champId) external{
        require(_isTrader(msg.sender, _champId));
        // Grab a reference to the matron in storage.
        ChampTrader storage champ = champTraders[_champId];

        champ.tradeLog = "";
        champ.totalTransactionQty = 0;
    }

}




