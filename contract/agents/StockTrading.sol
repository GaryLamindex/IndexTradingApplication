pragma solidity ^0.8.0;

contract StockActionAgent {
    struct ActionMsg{
        uint256 state;
        string ticker;
        string action;
        uint256 totalQuantity;
        uint256 avgPrice;
    }


    function placeBuyStockLimitOrderMsg(string tickerName,uint256 sharePurchase,uint256 limitPrice) public returns (ActionMsg){
        ActionMsg memory msg = ActionMsg({state:0,ticker: tickerName, action: 'buy', totalQuantity: sharePurchase, avgPrice: limitPrice});
        return msg;
    }

    function placeSellStockLimitOrderMsg(string tickerName,uint256 sharePurchase,uint256 limitPrice) public returns (ActionMsg){
        ActionMsg memory msg = ActionMsg({state:0,ticker: tickerName, action: 'sell', totalQuantity: sharePurchase, avgPrice: limitPrice});
        return msg;
    }
}
