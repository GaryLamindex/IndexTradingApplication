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

    string[] tickerNames;
    mapping (string => bool) tickerExists;

    mapping (string => bool) maxDrawdownDodge;
    mapping (string => int256) benchmarkDrawdownPrice;
    mapping (string => int256) liquidateTickerQty;
    mapping (string => int256) maxStockPrice;
    mapping (string => int256) liquidateTickerPrice;
    mapping (uint256 => mapping(string => RealTimeTickerData)) realTimeTickersData;

    mapping (uint256 => PortfolioData)  portfolioData;
    mapping (uint256 => mapping(string => ActionMsg)) actionMsgs;
    mapping (uint256 => mapping(string => PortfolioHolding)) portfolioHoldingsData;
    uint256 loop = 0;
    int256 rebalanceMargin = int256(5) * FixidityLib.fixed1() / int256(100);
    int256 maxDrawdown = int256(5) * FixidityLib.fixed1() / int256(1000);

    constructor(){
        tickerNames = ["QQQ", "SPY"];
        tickerExists["QQQ"] = true;
        tickerExists["SPY"] = true;
    }

    function inputRealTimeTickerData(string memory tickerName, int256 bidPrice, int256 last, uint256 timestamp) public {
        RealTimeTickerData memory tickerData = RealTimeTickerData(tickerName, bidPrice, last);
        realTimeTickersData[timestamp][tickerName] = tickerData;

        if(loop == 0){
            maxDrawdownDodge[tickerName] = false;
            benchmarkDrawdownPrice[tickerName] = 0;
            liquidateTickerQty[tickerName] = 0;
            maxStockPrice[tickerName] = 0;
            liquidateTickerPrice[tickerName] = 0;
        }

        if (!tickerExists[tickerName]){
            tickerNames.push(tickerName);
            tickerExists[tickerName] = true;
            maxDrawdownDodge[tickerName] = false;
            benchmarkDrawdownPrice[tickerName] = 0;
            liquidateTickerQty[tickerName] = 0;
            maxStockPrice[tickerName] = 0;
            liquidateTickerPrice[tickerName] = 0;
        }


    }

    function updatePortfolioData(MarginAccount memory marginAcc, TradingFunds memory tradingFunds, MktValue memory mktValue) public {
        int256 FullInitMarginReq = marginAcc.FullInitMarginReq;
        int256 FullMainMarginReq = marginAcc.FullMainMarginReq;
        int256 AvailableFunds = tradingFunds.AvailableFunds;
        int256 ExcessLiquidity = tradingFunds.ExcessLiquidity;
        int256 BuyingPower = tradingFunds.BuyingPower;
        int256 Leverage = tradingFunds.Leverage;
        int256 EquityWithLoanValue = tradingFunds.EquityWithLoanValue;
        int256 TotalCashValue = mktValue.TotalCashValue;
        int256 NetDividend = mktValue.NetDividend;
        int256 NetLiquidation = mktValue.NetLiquidation;
        int256 UnrealizedPnL = mktValue.UnrealizedPnL;
        int256 RealizedPnL = mktValue.RealizedPnL;
        int256 GrossPositionValue = mktValue.GrossPositionValue;

        PortfolioData memory data = PortfolioData(FullInitMarginReq, FullMainMarginReq, AvailableFunds, ExcessLiquidity, BuyingPower, Leverage, EquityWithLoanValue, TotalCashValue, NetDividend, NetLiquidation, UnrealizedPnL, RealizedPnL, GrossPositionValue);
        portfolioData[timestamp] = data;
    }

    function inputMarginAccount(int256 FullInitMarginReq, int256 FullMainMarginReq) public {
        MarginAccount memory data = MarginAccount(FullInitMarginReq, FullMainMarginReq);
    }

    function inputTradingFunds(int256 AvailableFunds, int256 ExcessLiquidity, int256 BuyingPower, int256 Leverage, int256 EquityWithLoanValue) public {
        TradingFunds memory data = TradingFunds(AvailableFunds, ExcessLiquidity, BuyingPower, Leverage, EquityWithLoanValue);
    }

    function inputMktValue(int256 TotalCashValue, int256 NetDividend, int256 NetLiquidation, int256 UnrealizedPnL, int256 RealizedPnL, int256 GrossPositionValue) public {
        MktValue memory data = MktValue(TotalCashValue, NetDividend, NetLiquidation, UnrealizedPnL, RealizedPnL, GrossPositionValue);
    }

    function updatePortfolioHoldingsData(string memory tickerName, int256 position, int256 marketPrice, int256 averageCost, int256 marketValue, int256 realizedPNL, int256 unrealizedPNL, int256 initMarginReq, int256 maintMarginReq, int256 costBasis) public {
        PortfolioHolding memory portfolioHolding = PortfolioHolding(tickerName, position, marketPrice, averageCost, marketValue, realizedPNL, unrealizedPNL, initMarginReq, maintMarginReq, costBasis);
        portfolioHoldingsData[timestamp][tickerName] = portfolioHolding;
    }

    function getTickerActionMsg(string memory tickerName, uint256 timestamp) public returns (string memory, uint256, string memory, int256, int256, int256){
        ActionMsg memory actionMsg = actionMsgs[timestamp][tickerName];
        return (actionMsg.tickerName, actionMsg.timestamp, actionMsg.transactionType, actionMsg.positionAction, actionMsg.transactionTickerPrice, actionMsg.transactionAmount);
    }

    function run(uint256 _timeStamp) public{
        require(checkExecution(timestamp));
        if(loop == 0){
            int256 capitalForEachStock = FixidityLib.divide(portfolioData[_timeStamp].TotalCashValue, int256(tickerNames.length) * FixidityLib.fixed1());
            for (uint i=0; i < tickerNames.length; i++){
                RealTimeTickerData memory tickerData = realTimeTickersData[_timeStamp][tickerNames[i]];
                string memory tickerName = tickerData.tickerName;
                int256 tickerPrice = tickerData.last;
                int256 sharePurchase = FixidityLib.divide(capitalForEachStock,tickerData.last) * 3;
                ActionMsg memory actionMsg = TradeAgent.placeBuyStockLimitOrderMsg(tickerName,sharePurchase,tickerPrice,_timeStamp);
                actionMsgs[timestamp][tickerName] = actionMsg;
            }
        }
        else{
            int256 targetExLiq = FixidityLib.multiply(rebalanceMargin, portfolioData[_timeStamp].GrossPositionValue);
            for (uint i=0; i < tickerNames.length; i++){
                int256 tickerPrice = realTimeTickersData[_timeStamp][tickerNames[i]].last;
                string memory tickerName = tickerNames[i];
                if (maxDrawdownDodge[tickerNames[i]]){
                    if (checkBuyBack(tickerName, tickerPrice)){
                        ActionMsg memory actionMsg = buyBackPosition(tickerName, tickerPrice,_timeStamp);
                        actionMsgs[timestamp][tickerName] = actionMsg;
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
                                actionMsgs[timestamp][tickerName] = actionMsg;
                            }
                        }
                    }
                }
            }

            for (uint i=0; i < tickerNames.length; i++){
                int256 tickerPrice = realTimeTickersData[_timeStamp][tickerNames[i]].last;
                string memory tickerName = tickerNames[i];
                if (maxDrawdownDodge[tickerName]){
                    updateBenchmarkDrawdownPriceAfterDodge(tickerName, tickerPrice);
                }
                else{
                    updateBenchmarkDrawdownPriceBeforeDodge(tickerName, tickerPrice);
                }
            }
        }
        lastExecutedTimestamp = _timeStamp;
        loop = loop + 1;

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
        PortfolioHolding memory holding = portfolioHoldingsData[_timeStamp][tickerName];
        RealTimeTickerData memory tickerData = realTimeTickersData[_timeStamp][tickerName];
        int256 currentPosition = holding.position;
        int256 noLeverageEquity = portfolioData[_timeStamp].EquityWithLoanValue / int256(tickerNames.length);
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

}
