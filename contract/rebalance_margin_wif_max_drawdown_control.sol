pragma solidity ^0.8.0;
import ".\utils\DateUtils.sol";
import ".\utils\FixidityLib.sol";
import ".\Object.sol";
import ".\agent\StockTrading.sol";
import ".\agents\Portfolio.sol";
import ".\agents\StockTrading.sol";

contract algorithm is Object, StockTrading{
    using DateUtils for *;
    using FixidityLib for int256;

    event LogActionMsg(string actionMsgs);
    ActionMsg[] memory actionMsgs;
    RealTimeTickerData[] realTimeTickerData;

    uint lastExecutedTimestamp;
    uint timestamp;
    PortfolioData portfolioData;
    mapping (string => bool) maxDrawdownDodge;
    mapping (string => int256) benchmarkDrawdownPrice;
    mapping (string => int256) liquidateTickerQty;
    mapping (string => int256) maxStockPrice;
    mapping (string => int256) liqSoldQty;
    mapping (string => int256) liqSoldPrice;
    uint256 loop = 0;
    uint256 rebalanceMargin = 5;
    uint256 maxDrawdown = 5;
    uint256 limitOrderAcceptanceRange;


    constructor(RealTimeTickerData[] memory _realTimeTickerData, uint256 _limitOrderAcceptanceRange){
        lastExecutedTimestamp = block.timestamp;
        limitOrderAcceptanceRange = _limitOrderAcceptanceRange;
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
            int256 capitalForEachStock = portfolioData.TotalCashValue / realTimeTickerData.length;
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
            uint256 targetExLiq = rebalanceMargin * portfolioData.GrossPositionValue;
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
                        ActionMsg memory actionMsg = liquidateTickerPosition(tickerName, tickerPrice * (1 - limitOrderAcceptanceRange),timestamp);
                    }
                    else{
                        if (portfolioData.ExcessLiquidity > targetExLiq) {
                            uint256 exLiqDiff = portfolioData.ExcessLiquidity - targetExLiq;
                            uint256 targetSharePurchase = exLiqDiff / tickerPrice;

                            if (targetSharePurchase > 0){
                                ActionMsg memory actionMsg = placeBuyStockLimitOrderMsg(tickerName, sharePurchase, limitPrice);
                                actionMsgs.push(actionMsg);
                            }
                        }
                    }
                }
            }

            for (uint i=0; i < realTimeTickerData.length; i++){
                uint256 tickerPrice = realTimeTickerData[i].last;
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

        delete realTimeTickerData;
        realTimeTickerData = _tickerData;

        portfolioData = _portfolioData;


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
        int256 rangePrice = benchmarkDrawdownPrice[ticker] * (100 + 1);
        int256 comparisonPrice = realTimeTickerPrice * 100;
        return comparisonPrice > rangePrice;
    }

    function buyBackPosition(string memory tickerName, uint256 limitPrice) internal returns (ActionMsg memory) {
        uint256 targetSharePurchase = liquidateTickerQty[ticker];
        uint256 purchaseAmount = targetSharePurchase * limitPrice;
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

    function checkMaxDrawdownDodge(string memory tickerName, uint256 limitPrice) returns bool{
        return limitPrice < benchmarkDrawdownPrice[ticker];
    }

    function updateBenchmarkDrawdownPriceAfterDodge(string memory tickerName, uint256 tickerPrice){
//        if real_time_ticker_price < self.benchmark_drawdown_price[ticker] * 0.7:
//            target_update_benchmark_drawdown_price = real_time_ticker_price + (
//                    (self.liq_sold_price_dict[ticker] - real_time_ticker_price) * 0.5)
//            if target_update_benchmark_drawdown_price < self.benchmark_drawdown_price[ticker]:
//                self.benchmark_drawdown_price[ticker] = target_update_benchmark_drawdown_price

        if (limitPrice < benchmarkDrawdownPrice[tickerName] * 0.7){
            uint256 targetUpdateBenchmarkDrawdownPrice = tickerPrice + (liqSoldPrice[tickerName] - tickerPrice) * 0.5;
            if targetUpdateBenchmarkDrawdownPrice < benchmarkDrawdownPrice[tickerName] :
            benchmarkDrawdownPrice[tickerName] = targetUpdateBenchmarkDrawdownPrice;
        }
    }

    function updateBenchmarkDrawdownPriceBeforeDodge(string memory tickerName, uint256 limitPrice){
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
            benchmarkPrice = limitPrice * (1 - maxDrawdown);
            benchmarkDrawdownPrice[ticker] = benchmark_price;
        }
        else if (limitPrice > maxStockPrice[ticker]){
            maxStockPrice[ticker] = limitPrice;
            benchmarkDrawdownPrice[ticker] = limitPrice * (1 - maxDrawdown);
        }

    }

    function liquidateTickerPosition(string memory tickerName, uint256 limitPrice) returns ActionMsg{
//        current_position = float(self.account_snapshot.get(f"{ticker} position"))
//        no_leverage_position = (float(
//            self.account_snapshot.get("EquityWithLoanValue")) / self.number_of_stocks) / float(
//            self.account_snapshot.get(f"{ticker} marketPrice"))
//        target_leverage_position_at_liquid_state = no_leverage_position * 1.2
//        print("liquidate_stock_position::current_position:", current_position)
//        print("liquidate_stock_position::no_leverage_position:", current_position)
//        target_sold_position = math.ceil(current_position - target_leverage_position_at_liquid_state)
//
//        action_msg = self.trade_agent.place_sell_stock_limit_order(ticker, target_sold_position, limit_price, timestamp)
//        self.action_msgs.append(action_msg)
//
//        if int(action_msg['state']) == 1:  # successfully placed the sell order
//            self.benchmark_drawdown_price.update({ticker: action_msg['avgPrice']})
//            self.liq_sold_qty_dict.update({ticker: action_msg['totalQuantity']})
//            self.liq_sold_price_dict.update({ticker: action_msg['avgPrice']})
//            self.liquidate_sold_value.update({ticker: action_msg['avgPrice'] * action_msg['totalQuantity']})
//
//        self.max_drawdown_dodge.update({ticker: True})

        uint256 currentPosition = portfolioData.tickerPortfolio[ticker];
        uint256 noLeveragePosition = portfolioData.EquityWithLoanValue / realTimeTickerData.length / realTimeTickerData[tickerName];
        uint256 targetLeveragePositionAtLiquidState = noLeveragePosition * 12;

        uint256 targetSoldPosition = currentPosition - targetLeveragePositionAtLiquidState;
        ActionMsg memory actionMsg = placeSellStockLimitOrderMsg(tickerName,targetSoldPosition,limitPrice);

        benchmarkDrawdownPrice[ticker] = limitPrice;
        liqSoldQty[ticker] = targetSoldPosition;
        liqSoldPrice[ticker] = limitPrice;
        maxDrawdownDodge[ticker] = true;

        return actionMsg;
    }
}
