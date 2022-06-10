pragma solidity ^0.8.0;
import ".\utils\DateUtils.sol";
import ".\utils\FixidityLib.sol";
import ".\Object.sol";
import ".\agent\StockTrading.sol";
import ".\agents\Portfolio.sol";
import ".\agents\StockTrading.sol";

contract algorithm is Object, StockTrading{
    using DateUtils for *;

    event LogActionMsg(string actionMsgs);
    ActionMsg[] memory actionMsgs;

    uint lastExecutedTimestamp;
    uint timestamp;
    mapping (string => bool) maxDrawdownDodge;
    mapping (string => int256) benchmarkDrawdownPrice;
    mapping (string => int256) liquidateTickerQty;
    mapping (string => int256) maxStockPrice;
    mapping (string => int256) liqSoldQty;
    mapping (string => int256) liqSoldPrice;
    mapping (uint256 => RealTimeTickerData[]) realTimeTickerData;
    mapping (uint256 => PortfolioData)  portfolioData;
    uint256 loop = 0;
    int256 rebalanceMargin = 5/100 * FixidityLib.fixed1();
    int256 maxDrawdown = 5/1000 * FixidityLib.fixed1();

    constructor(RealTimeTickerData[] memory _realTimeTickerData){
        lastExecutedTimestamp = block.timestamp;
        for (uint i=0; i < _realTimeTickerData.length; i++){
            string tickerName = realTimeTickerData[i].tickerName;
            maxDrawdownDodge[tickerName] = false;
        }
    }

    function run(RealTimeTickerData[] memory _tickerData, uint256 _timeStamp, PortfolioData memory _portfolioData) public returns (ActionMsg[] memory){
        ActionMsg[] actionMsgs;
        require(checkExecution(timestamp));
        renewContractState(_tickerData, _timeStamp, _portfolioData);
        if(loop == 0){
            int256 capitalForEachStock = FixidityLib.divide(portfolioData.TotalCashValue, realTimeTickerData.length);
            for (uint i=0; i < realTimeTickerData.length; i++){
                RealTimeTickerData data = realTimeTickerData[i];
                string tickerName = data.tickerName;
                uint256 limitPrice = data.last;
                uint256 sharePurchase = capitalForEachStock/data.last * 3;
                ActionMsg actionMsg = placeBuyStockLimitOrderMsg(tickerName, sharePurchase, limitPrice);
                actionMsgs.push(actionMsg);
            }
        }
        else{
            int256 targetExLiq = FixidityLib.multiply(rebalanceMargin, portfolioData.GrossPositionValue);
            for (uint i=0; i < realTimeTickerData.length; i++){
                int256 tickerPrice = realTimeTickerData[i].last;
                string tickerName = realTimeTickerData[i].tickerName;
                if (maxDrawdownDodge[realTimeTickerData[i].tickerName]){
                    int256 buyingPower = portfolioData.BuyingPower;
                    if (checkBuyBack(tickerName, tickerPrice)){
                        ActionMsg memory actionMsg = buyBackPosition(tickerName, tickerPrice, buyingPower,timestamp);
                        actionMsgs.push(actionMsg);
                    }
                }else{
                    if(checkMaxDrawdownDodge(tickerName,tickerPrice)){
                        ActionMsg memory actionMsg = liquidateTickerPosition(tickerName, tickerPrice ,timestamp);
                    }
                    else{
                        if (portfolioData.ExcessLiquidity > targetExLiq) {
                            int256 exLiqDiff = FixidityLib.subtract(portfolioData.ExcessLiquidity, targetExLiq);
                            int256 targetSharePurchase = FixidityLib.divide(exLiqDiff,tickerPrice);

                            if (targetSharePurchase > 0){
                                ActionMsg memory actionMsg = placeBuyStockLimitOrderMsg(tickerName, sharePurchase, limitPrice);
                                actionMsgs.push(actionMsg);
                            }
                        }
                    }
                }
            }

            for (uint i=0; i < realTimeTickerData.length; i++){
                int256 tickerPrice = realTimeTickerData[i].last;
                string tickerName = realTimeTickerData[i].tickerName;
                if (maxDrawdownDodge[realTimeTickerData.tickerName]){
                    updateBenchmarkDrawdownPriceAfterDodge(tickerName, tickerPrice);
                }
                else{
                    updateBenchmarkDrawdownPriceBeforeDodge(tickerName, tickerPrice);
                }
            }
        }
        lastExecutedTimestamp = block.timestamp;
        loop = loop + 1;
        return actionMsgs;
    }

    function renewContractState(RealTimeTickerData[] memory _tickerData, uint256 _timeStamp, PortfolioData memory _portfolioData){
        timestamp = _timeStamp;
        realTimeTickerData[_timeStamp] = _tickerData;
        portfolioData[_timeStamp] = _portfolioData;
    }

    function checkExecution(uint256 _timestamp) internal returns (bool){
        bool exec = false;
        (_year, _month, _day) = _timestamp.convertTimestampToYMD;
        (_lastYear, _lastMonth, _lastday) = lastExecutedTimestamp.convertTimestampToYMD;
        if (_month != _lastMonth){
            exec = true;
            lastExecutedTimestamp = _timestamp;
            }
        return exec;
    }


    function checkBuyBack(string memory ticker, uint256 realTimeTickerPrice)internal returns (bool){
//        range_price = self.benchmark_drawdown_price.get(ticker) * (1 + 0.01)
//        return ticker_price > range_price
        int256 rangePrice = benchmarkDrawdownPrice[ticker] /100*101;
        return realTimeTickerPrice > rangePrice;
    }

    function buyBackPosition(string memory tickerName, uint256 limitPrice) internal returns (ActionMsg memory) {
        int256 targetSharePurchase = liquidateTickerQty[ticker];
        int256 purchaseAmount = targetSharePurchase * limitPrice;
        if (portfolioData.BuyingPower >= purchaseAmount){
            ActionMsg memory actionMsg = placeBuyStockLimitOrderMsg(tickerName, targetSharePurchase, limitPrice);
        }else{
            targetSharePurchase = portfolioData.BuyingPower / limitPrice;
            ActionMsg memory actionMsg = placeBuyStockLimitOrderMsg(tickerName, targetSharePurchase, limitPrice);
        }

        maxDrawdownDodge[ticker] = False;
        maxStockPrice[ticker] = limitPrice;

        return actionMsg;
    }

    function checkMaxDrawdownDodge(string memory tickerName, int256 limitPrice) returns (bool){
        return limitPrice < benchmarkDrawdownPrice[ticker];
    }

    function updateBenchmarkDrawdownPriceAfterDodge(string memory tickerName, int256 tickerPrice){
//        if real_time_ticker_price < self.benchmark_drawdown_price[ticker] * 0.7:
//            target_update_benchmark_drawdown_price = real_time_ticker_price + (
//                    (self.liq_sold_price_dict[ticker] - real_time_ticker_price) * 0.5)
//            if target_update_benchmark_drawdown_price < self.benchmark_drawdown_price[ticker]:
//                self.benchmark_drawdown_price[ticker] = target_update_benchmark_drawdown_price

        if (tickerPrice < benchmarkDrawdownPrice[tickerName] * (70 / 100)){
            int256 tickerDiff = FixidityLib.subtract(liqSoldPrice[tickerName],tickerPrice);
            int256 targetUpdateBenchmarkDrawdownPrice = FixidityLib.add(tickerPrice, tickerDiff) * 50 / 100;
            if (targetUpdateBenchmarkDrawdownPrice < benchmarkDrawdownPrice[tickerName]){
                benchmarkDrawdownPrice[tickerName] = targetUpdateBenchmarkDrawdownPrice;
            }
        }
    }

    function updateBenchmarkDrawdownPriceBeforeDodge(string memory tickerName, int256 realTimeTickerPrice){
//        if (self.max_stock_price.get(ticker) == 0):  # if there is no data for the max and benchmark price
//            self.max_stock_price.update({ticker: real_time_ticker_price})
//            benchmark_price = real_time_ticker_price * (1 - self.max_drawdown_ratio)
//            self.benchmark_drawdown_price.update({ticker: benchmark_price})
//        elif real_time_ticker_price > self.max_stock_price.get(ticker):
//            self.max_stock_price.update({ticker: real_time_ticker_price})
//            benchmark_price = real_time_ticker_price * (1 - self.max_drawdown_ratio)
//            self.benchmark_drawdown_price.update({ticker: benchmark_price})

        if (maxStockPrice[ticker] == 0){
            maxStockPrice[ticker] = realTimeTickerPrice;
            int256 drawDownFactor = FixidityLib.subtract(FixidityLib.fixed1(), maxDrawdown);
            int256 benchmarkPrice = FixidityLib.multiply(maxStockPrice[ticker], drawDownFactor);
            benchmarkDrawdownPrice[ticker] = benchmark_price;
        }
        else if (realTimeTickerPrice > maxStockPrice[ticker]){
            maxStockPrice[ticker] = realTimeTickerPrice;
            int256 drawDownFactor = FixidityLib.subtract(FixidityLib.fixed1(), maxDrawdown);
            int256 benchmarkPrice = FixidityLib.multiply(maxStockPrice[ticker], drawDownFactor);
            benchmarkDrawdownPrice[ticker] = benchmarkPrice;
        }

    }

    function liquidateTickerPosition(string memory tickerName, uint256 limitPrice) returns (ActionMsg){

        int256 currentPosition = portfolioData.tickerPortfolio[ticker];
        int256 noLeverageEquity = portfolioData.EquityWithLoanValue / realTimeTickerData.length;
        int256 noLeveragePosition = FixidityLib.divide(noLeverageEquity,realTimeTickerData[tickerName]);
        int256 targetLeveragePositionAtLiquidState = noLeveragePosition * 12 / 10;

        int256 targetSoldPosition = currentPosition - targetLeveragePositionAtLiquidState;
        ActionMsg memory actionMsg = placeSellStockLimitOrderMsg(tickerName,targetSoldPosition,limitPrice);

        benchmarkDrawdownPrice[ticker] = limitPrice;
        liqSoldQty[ticker] = targetSoldPosition;
        liqSoldPrice[ticker] = limitPrice;
        maxDrawdownDodge[ticker] = true;

        return actionMsg;
    }
}
