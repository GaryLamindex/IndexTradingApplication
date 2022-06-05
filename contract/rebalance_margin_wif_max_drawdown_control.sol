pragma solidity ^0.8.0;
import "./utils/DateUtils.sol";
import "./Object.sol";
import "./agent/StockTrading.sol";
import "./agents/Portfolio.sol";

contract algorithm is Object, StockTrading{
    using DateUtils for *;
    event LogActionMsg(string actionMsgs);

    RealTimeTickerData[] realTimeTickerData;

    uint lastExecutedTimestamp;
    uint timestamp;
    PortfolioData portfolioData;
    mapping (string => bool) maxDrawdownDodge;
    uint256 loop = 0;
    uint256 rebalanceMargin = 5;
    uint256 maxDrawdown = 5;
    uint256 limitOrderAcceptanceRange;

    constructor(RealTimeTickerData[] memory _tickerData, uint256 _limitOrderAcceptanceRange){
        lastExecutedTimestamp = block.timestamp;
        limitOrderAcceptanceRange = _limitOrderAcceptanceRange;
        for (uint i=0; i < realTimeTickerData.length; i++){
            string tickerName = realTimeTickerData[i].tickerName;
            maxDrawdownDodge[tickerName] = false;
        }
    }

    function run(RealTimeTickerData[] memory _tickerData, uint256 _timeStamp, PortfolioData memory _portfolioData) external returns ActionMsg[]{
        ActionMsg[] actionMsgs;
        uint256 capitalForEachStock = 0;
        require(checkExecution(timestamp));

        renewContractState(_tickerData, _timeStamp, _portfolioData);
        if loop == 0:
            capitalForEachStock = portfolioData.TotalCashValue / realTimeTickerData.length
            for (uint i=0; i < realTimeTickerData.length; i++){
                RealTimeTickerData data = realTimeTickerData[i];
                string tickerName = data.tickerName;
                uint256 limitPrice = data.last;
                uint256 sharePurchase = capitalForEachStock/data.last * 3;
                ActionMsg actionMsg = placeBuyStockLimitOrderMsg(tickerName, sharePurchase, limitPrice);
                actionMsgs.push(actionMsg);
            }
        else:
            uint256 targetExLiq = rebalanceMargin * portfolioData.GrossPositionValue;
            for (uint i=0; i < realTimeTickerData.length; i++){
                uint256 tickerPrice = realTimeTickerData[i].last;
                string tickerName = realTimeTickerData[i].tickerName;
                if maxDrawdownDodge[realTimeTickerData.tickerName]:
                    uint256 buyingPower = portfolioData.BuyingPower;
                    if checkBuyBack(tickerName, tickerPrice):
                        buyBackPosition(tickerName, tickerPrice, buyingPower,timestamp);
                else:
                    if checkMaxDrawdownDodge(tickerName, tickerPrice):
                        maxDrawdownDodge(tickerName, tickerPrice * (1 - limitOrderAcceptanceRange),timestamp);
                    else:
                        if portfolioData.ExcessLiquidity > targetExLiq:
                            uint256 exLiqDiff = portfolioData.ExcessLiquidity - targetExLiq;
                            uint256 targetSharePurchase = exLiqDiff / tickerPrice;

                            if targetSharePurchase > 0:
                                ActionMsg actionMsg = placeBuyStockLimitOrderMsg(tickerName, sharePurchase, limitPrice);
                                actionMsgs.push(actionMsg);
            }

            for (uint i=0; i < realTimeTickerData.length; i++){
                uint256 tickerPrice = realTimeTickerData[i].last;
                string tickerName = realTimeTickerData[i].tickerName;
                if maxDrawdownDodge[realTimeTickerData.tickerName]:
                    updateBenchmarkDrawdownPriceAfterDodge(tickerName, tickerPrice);
                else:
                    updateBenchmarkDrawdownPriceBeforeDodge(tickerName, tickerPrice);
            }

        lastExecutedTimestamp = block.timestamp;

        return actionMsgs;
    }

    function renewContractState(RealTimeTickerData[] memory _tickerData, uint256 _timeStamp, PortfolioData memory _portfolioData){
        timestamp = _timeStamp;

        delete realTimeTickerData;
        realTimeTickerData = _tickerData;

        portfolioData = _portfolioData;


    }

    function checkExecution(uint256 _timestamp) internal returns bool{
        bool exec = false;
        (_year, _month, _day) = _timestamp.convertTimestampToYMD;
        (_lastYear, _lastMonth, _lastday) = lastExecutedTimestamp.convertTimestampToYMD;
        if _month != _lastMonth:
            exec = true;
            lastExecutedTimestamp = _timestamp
        return exec;
    }
}
