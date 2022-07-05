pragma solidity ^0.8.13;
import "./utils/DateUtils.sol";
import "./utils/FixidityLib.sol";
import "./agents/TradeAgent.sol";
import "./Object.sol";

contract RebalanceMarginWifMaxDrawdownControl is Object, TradeAgent{
    using DateUtils for *;
    using StringUtils for string;
    event LogActionMsg(string actionMsgs);

    uint256 lastExecutedTimestamp;
    uint256 timestamp;
    mapping (string => bool) maxDrawdownDodge;
    mapping (string => int256) benchmarkDrawdownPrice;
    mapping (string => int256) liquidateTickerQty;
    mapping (string => int256) maxStockPrice;
    mapping (string => int256) liquidateTickerPrice;
    mapping (uint256 => RealTimeTickerData[]) realTimeTickersData;
    mapping (uint256 => PortfolioData)  portfolioData;
    mapping (uint256 => ActionMsg[]) actionMsgs;
    uint256 loop = 0;
    int256 rebalanceMargin = int256(5) * FixidityLib.fixed1() / int256(100);
    int256 maxDrawdown = int256(5) * FixidityLib.fixed1() / int256(1000);

    constructor(){

    }

    function inputRealTimeTickerData(string tickerName, int256 bidPrice, int256 last, uint256 timestamp) public {
        RealTimeTickerData memory tickerData = RealTimeTickerData(tickerName, bidPrice, last);
        realTimeTickersData[timestamp].push(tickerData);
    }

    function inputPortfolioData(string memory AccountCode, string memory Currency, string memory ExchangeRate, int256 FullInitMarginReq, int256 FullMainMarginReq, int256 AvailableFunds, int256 ExcessLiquidity, int256 BuyingPower, int256 Leverage, int256 EquityWithLoanValue, int256 TotalCashValue, int256 NetDividend, int256 NetLiquidation, int256 UnrealizedPnL, int256 RealizedPnL, int256 GrossPositionValue) public {
        PortfolioData memory portfolioData = PortfolioData(AccountCode, Currency, ExchangeRate, FullInitMarginReq, FullMainMarginReq, AvailableFunds, ExcessLiquidity, BuyingPower, Leverage, EquityWithLoanValue, TotalCashValue, NetDividend, NetLiquidation, UnrealizedPnL, RealizedPnL, GrossPositionValue);
        portfolioData[timestamp] = portfolioData;
    }

    function updatePortfolioHoldingsData(string memory tickerName, int256 position, int256 marketPrice, int256 averageCost, int256 marketValue, int256 realizedPNL, int256 unrealizedPNL, int256 initMarginReq, int256 maintMarginReq, int256 costBasis) public {
        PortfolioHolding memory portfolioHolding = PortfolioHolding(tickerName, position, marketPrice, averageCost, marketValue, realizedPNL, unrealizedPNL, initMarginReq, maintMarginReq, costBasis);
        portfolioHoldingsData[timestamp].push(portfolioHolding);
    }

    function getTickerActionMsg(string tickerName, uint256 timestamp) public {
        ActionMsg memory actionMsg = actionMsgs[timestamp];
        return (actionMsg.tickerName, actionMsg.timestamp, actionMsg.positionAction, actionMsg.transactionTickerPrice, actionMsg.transactionAmount);
    }

    function run(uint256 _timeStamp) public returns (ActionMsg[] memory){

        require(checkExecution(timestamp));
        renewContractState(_tickerData, _timeStamp, _portfolioData, _portfolioHoldings);
        if(loop == 0){
            int256 capitalForEachStock = FixidityLib.divide(portfolioData[_timeStamp].TotalCashValue, int256(realTimeTickersData[_timeStamp].length));
            for (uint i=0; i < realTimeTickersData[_timeStamp].length; i++){
                RealTimeTickerData memory tickerData = realTimeTickersData[_timeStamp][i];
                string memory tickerName = tickerData.tickerName;
                int256 tickerPrice = tickerData.last;
                int256 sharePurchase = FixidityLib.divide(capitalForEachStock,tickerData.last) * 3;
                ActionMsg memory actionMsg = TradeAgent.placeBuyStockLimitOrderMsg(tickerName,sharePurchase,tickerPrice,_timeStamp);
                actionMsgs[timestamp].push(actionMsg);
            }
        }
        else{
            int256 targetExLiq = FixidityLib.multiply(rebalanceMargin, portfolioData[_timeStamp].GrossPositionValue);
            for (uint i=0; i < realTimeTickersData[_timeStamp].length; i++){
                int256 tickerPrice = realTimeTickersData[_timeStamp][i].last;
                string memory tickerName = realTimeTickersData[_timeStamp][i].tickerName;
                if (maxDrawdownDodge[realTimeTickersData[_timeStamp][i].tickerName]){
                    if (checkBuyBack(tickerName, tickerPrice)){
                        ActionMsg memory actionMsg = buyBackPosition(tickerName, tickerPrice,_timeStamp);
                        actionMsgs[timestamp].push(actionMsg);
                    }
                }else{
                    if(checkMaxDrawdownDodge(tickerName,tickerPrice)){
                        ActionMsg memory actionMsg = liquidateTickerPosition(tickerName, tickerPrice ,_timeStamp);
                    }
                    else{
                        if (portfolioData[_timeStamp].ExcessLiquidity > targetExLiq) {
                            int256 exLiqDiff = FixidityLib.subtract(portfolioData[_timeStamp].ExcessLiquidity, targetExLiq);
                            int256 targetSharePurchase = FixidityLib.divide(exLiqDiff,tickerPrice);

                            if (targetSharePurchase > 0){
                                ActionMsg memory actionMsg = super.placeBuyStockLimitOrderMsg(tickerName,targetSharePurchase,tickerPrice,_timeStamp);
                                actionMsgs[timestamp].push(actionMsg);
                            }
                        }
                    }
                }
            }

            for (uint i=0; i < realTimeTickersData[_timeStamp].length; i++){
                int256 tickerPrice = realTimeTickersData[_timeStamp][i].last;
                string memory tickerName = realTimeTickersData[_timeStamp][i].tickerName;
                if (maxDrawdownDodge[realTimeTickersData[_timeStamp][i].tickerName]){
                    updateBenchmarkDrawdownPriceAfterDodge(tickerName, tickerPrice);
                }
                else{
                    updateBenchmarkDrawdownPriceBeforeDodge(tickerName, tickerPrice);
                }
            }
        }
        lastExecutedTimestamp = _timeStamp;
        loop = loop + 1;

        ActionMsg[] memory msgs = new ActionMsg[](actionMsgs[timestamp].length);
        for (uint i = 0; i < actionMsgs[timestamp].length; i++) {
            msgs[i] = actionMsgs[timestamp][i];
        }

        return msgs;
    }

    function renewContractState(RealTimeTickerData[] memory _tickersData, uint256 _timeStamp, PortfolioData memory _portfolioData, PortfolioHolding[] memory _portfolioHoldings) private{
        timestamp = _timeStamp;

        //        lastExecutedTimestamp = block.timestamp;
        if(loop == 0){
            for (uint i=0; i < _tickersData.length; i++){
                RealTimeTickerData memory tickerData = _tickersData[i];
                string memory tickerName = tickerData.tickerName;
                maxDrawdownDodge[tickerName] = false;
                benchmarkDrawdownPrice[tickerName] = 0;
                liquidateTickerQty[tickerName] = 0;
                maxStockPrice[tickerName] = 0;
                liquidateTickerPrice[tickerName] = 0;
            }
        }


        RealTimeTickerData[] storage _realTimeTickerArray = realTimeTickersData[_timeStamp];
        for (uint i = 0; i < _tickersData.length; i++) {
            RealTimeTickerData memory _tickerData = _tickersData[i];
            _realTimeTickerArray[i].bidPrice =_tickerData.bidPrice;
            _realTimeTickerArray[i].last =_tickerData.last;
            _realTimeTickerArray[i].tickerName =_tickerData.tickerName;
        }

        PortfolioHolding[] storage _portfolioHoldingArray = portfolioHoldingsData[_timeStamp];
        for (uint i = 0; i < _portfolioHoldings.length; i++) {
            PortfolioHolding memory _portfolioHolding = _portfolioHoldings[i];
            _portfolioHoldingArray[i].tickerName = _portfolioHolding.tickerName;
            _portfolioHoldingArray[i].averageCost = _portfolioHolding.averageCost;
            _portfolioHoldingArray[i].costBasis = _portfolioHolding.costBasis;
            _portfolioHoldingArray[i].initMarginReq = _portfolioHolding.initMarginReq;
            _portfolioHoldingArray[i].maintMarginReq = _portfolioHolding.maintMarginReq;
            _portfolioHoldingArray[i].marketPrice = _portfolioHolding.marketPrice;
            _portfolioHoldingArray[i].marketValue = _portfolioHolding.marketValue;
            _portfolioHoldingArray[i].position = _portfolioHolding.position;
            _portfolioHoldingArray[i].realizedPNL = _portfolioHolding.realizedPNL;
            _portfolioHoldingArray[i].unrealizedPNL = _portfolioHolding.unrealizedPNL;
        }

        portfolioData[_timeStamp] = _portfolioData;
        delete actionMsgs;
    }

    function checkExecution(uint256 _timestamp) internal returns (bool){

        bool exec = false;
        uint16 _year;
        uint8 _month;
        uint8 _day;

        uint16 _lastYear;
        uint8 _lastMonth;
        uint8 _lastday;

        (_year, _month, _day) = DateUtils.convertTimestampToYMD(_timestamp);
        (_lastYear, _lastMonth, _lastday) = DateUtils.convertTimestampToYMD(lastExecutedTimestamp);
        if (_month != _lastMonth){
            exec = true;
            lastExecutedTimestamp = _timestamp;
            }
        return exec;
    }


    function checkBuyBack(string memory tickerName, int256 realTimeTickerPrice)internal returns (bool){
//        range_price = self.benchmark_drawdown_price.get(ticker) * (1 + 0.01)
//        return ticker_price > range_price
        int256 rangePrice = benchmarkDrawdownPrice[tickerName] /100*101;
        return realTimeTickerPrice > rangePrice;
    }

    function buyBackPosition(string memory tickerName, int256 tickerPrice, uint256 _timeStamp) internal returns (ActionMsg memory) {
        ActionMsg memory actionMsg;
        int256 targetSharePurchase = liquidateTickerQty[tickerName];
        int256 purchaseAmount =  FixidityLib.multiply(targetSharePurchase, tickerPrice);
        if (portfolioData[_timeStamp].BuyingPower >= purchaseAmount){
            actionMsg = super.placeBuyStockLimitOrderMsg(tickerName,targetSharePurchase,tickerPrice,_timeStamp);
        }else{
            targetSharePurchase = portfolioData[_timeStamp].BuyingPower / tickerPrice;
            actionMsg = super.placeBuyStockLimitOrderMsg(tickerName,targetSharePurchase,tickerPrice,_timeStamp);
        }

        maxDrawdownDodge[tickerName] = false;
        maxStockPrice[tickerName] = tickerPrice;

        return actionMsg;
    }


    function checkMaxDrawdownDodge(string memory tickerName, int256 tickerPrice) private returns (bool){
        return tickerPrice < benchmarkDrawdownPrice[tickerName];
    }

    function updateBenchmarkDrawdownPriceAfterDodge(string memory tickerName, int256 tickerPrice) private{
//        if real_time_ticker_price < self.benchmark_drawdown_price[ticker] * 0.7:
//            target_update_benchmark_drawdown_price = real_time_ticker_price + (
//                    (self.liq_sold_price_dict[ticker] - real_time_ticker_price) * 0.5)
//            if target_update_benchmark_drawdown_price < self.benchmark_drawdown_price[ticker]:
//                self.benchmark_drawdown_price[ticker] = target_update_benchmark_drawdown_price

        int256 benchmarkDrawdownFactor = int256(70) * FixidityLib.fixed1() / int256(100);
        if (tickerPrice < FixidityLib.multiply(benchmarkDrawdownPrice[tickerName] , benchmarkDrawdownFactor)){

            int256 tickerDiff = FixidityLib.subtract(liquidateTickerPrice[tickerName],tickerPrice);
            int256 targetUpdateBenchmarkDrawdownPrice = FixidityLib.add(tickerPrice, tickerDiff) * 50 / 100;
            if (targetUpdateBenchmarkDrawdownPrice < benchmarkDrawdownPrice[tickerName]){
                benchmarkDrawdownPrice[tickerName] = targetUpdateBenchmarkDrawdownPrice;
            }
        }
    }

    function updateBenchmarkDrawdownPriceBeforeDodge(string memory tickerName, int256 realTimeTickerPrice) private{
//        if (self.max_stock_price.get(ticker) == 0):  # if there is no data for the max and benchmark price
//            self.max_stock_price.update({ticker: real_time_ticker_price})
//            benchmark_price = real_time_ticker_price * (1 - self.max_drawdown_ratio)
//            self.benchmark_drawdown_price.update({ticker: benchmark_price})
//        elif real_time_ticker_price > self.max_stock_price.get(ticker):
//            self.max_stock_price.update({ticker: real_time_ticker_price})
//            benchmark_price = real_time_ticker_price * (1 - self.max_drawdown_ratio)
//            self.benchmark_drawdown_price.update({ticker: benchmark_price})

        if (maxStockPrice[tickerName] == 0){
            maxStockPrice[tickerName] = realTimeTickerPrice;
            int256 drawDownFactor = FixidityLib.subtract(FixidityLib.fixed1(), maxDrawdown);
            int256 tickerBenchmarkPrice = FixidityLib.multiply(maxStockPrice[tickerName], drawDownFactor);
            benchmarkDrawdownPrice[tickerName] = tickerBenchmarkPrice;
        }
        else if (realTimeTickerPrice > maxStockPrice[tickerName]){
            maxStockPrice[tickerName] = realTimeTickerPrice;
            int256 drawDownFactor = FixidityLib.subtract(FixidityLib.fixed1(), maxDrawdown);
            int256 tickerBenchmarkPrice = FixidityLib.multiply(maxStockPrice[tickerName], drawDownFactor);
            benchmarkDrawdownPrice[tickerName] = tickerBenchmarkPrice;
        }

    }

    function liquidateTickerPosition(string memory tickerName, int256 tickerPrice, uint256 _timeStamp) private returns (ActionMsg memory){
        PortfolioHolding memory holding = getPortfolioHoldingByTickerName(tickerName, portfolioHoldingsData[_timeStamp]);
        RealTimeTickerData memory tickerData = getRealTimeTickerDataByTickerName(tickerName, realTimeTickersData[_timeStamp]);
        int256 currentPosition = holding.position;
        int256 noLeverageEquity = portfolioData[_timeStamp].EquityWithLoanValue / int256(realTimeTickersData[_timeStamp].length);
        int256 noLeveragePosition = FixidityLib.divide(noLeverageEquity,tickerData.last);
        int256 targetLeveragePositionAtLiquidState = noLeveragePosition * 12 / 10;

        int256 targetSoldPosition = currentPosition - targetLeveragePositionAtLiquidState;
        ActionMsg memory actionMsg = super.placeSellStockLimitOrderMsg(tickerName,targetSoldPosition,tickerPrice, _timeStamp);

        benchmarkDrawdownPrice[tickerName] = tickerPrice;
        liquidateTickerQty[tickerName] = targetSoldPosition;
        liquidateTickerPrice[tickerName] = tickerPrice;
        maxDrawdownDodge[tickerName] = true;

        return actionMsg;
    }

    function getPortfolioHoldingByTickerName(string memory tickerName, PortfolioHolding[] memory holdings) private returns (PortfolioHolding memory){
        PortfolioHolding memory holding;
        for(uint i=0; i< holdings.length; i++){
            if(holdings[i].tickerName.compareStrings(tickerName)){
                holding = holdings[i];
            }
        }
        return holding;
    }

    function getRealTimeTickerDataByTickerName(string memory tickerName,  RealTimeTickerData[] memory realTimeTickersData) private returns (RealTimeTickerData memory){
        RealTimeTickerData memory realTimeTickerData;
        for (uint i=0; i<realTimeTickersData.length; i++){
            if(realTimeTickersData[i].tickerName.compareStrings(tickerName)){
                realTimeTickerData = realTimeTickersData[i];
            }
        }
        return realTimeTickerData;
    }
}
