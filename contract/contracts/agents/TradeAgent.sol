pragma solidity ^0.8.0;
import "../Object.sol";
import "../utils/FixidityLib.sol";

library TradeAgent is Object{
    function placeBuyStockLimitOrderMsg(string memory _tickerName, int256 _targetSharePurchase, int256 _transactionTickerPrice, uint256 _timestamp) public returns (ActionMsg memory){
        int256 _transactionAmount = FixidityLib.multiply(_targetSharePurchase, _transactionTickerPrice);
        ActionMsg memory buy_msg = ActionMsg({tickerName:_tickerName, timestamp: _timestamp, transactionType: 'Buy', positionAction: _targetSharePurchase, transactionTickerPrice:_transactionTickerPrice, transactionAmount:_transactionAmount});

        return buy_msg;
    }

    function placeSellStockLimitOrderMsg(string memory _tickerName, int256 _targetShareSell, int256 _transactionTickerPrice, uint256 _timestamp) public returns (ActionMsg memory){
        int256 _transactionAmount = FixidityLib.multiply(_targetShareSell, _transactionTickerPrice);
        ActionMsg memory sell_msg = ActionMsg({tickerName:_tickerName, timestamp: _timestamp, transactionType: 'Sell', positionAction: _targetShareSell, transactionTickerPrice:_transactionTickerPrice, transactionAmount:_transactionAmount});

        return sell_msg;
    }


}
