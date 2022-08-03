pragma solidity ^0.8.13;
import "./utils/DateUtils.sol";
import "./utils/FixidityLib.sol";
import "./agents/TradeAgent.sol";
import "./Object.sol";

contract RebalanceMarginWifMaxDrawdownControl is Object, TradeAgent{
    using DateUtils for *;
    using StringUtils for string;
    event LogTradeActionMsg(string tradeActionMsg);

    uint256 lastExecutedTimestamp;

    string[] tickerNames;
    uint256[] executionTimestamps;

    mapping (string => bool) tickerExists;

    mapping (string => bool) maxDrawdownDodge;
    mapping (string => int256) benchmarkDrawdownPrice;
    mapping (string => int256) liquidateTickerQty;
    mapping (string => int256) maxStockPrice;
    mapping (string => int256) liquidateTickerPrice;
    mapping (uint256 => mapping(string => RealTimeTickerData)) realTimeTickersData;

    mapping(uint256 => MarginAccount) marginAccountData;
    mapping(uint256 => TradingFunds) tradingFundsData;
    mapping(uint256 => MktValue) mktValueData;

    mapping (uint256 => mapping(string => TradeActionMsg)) tradeActionMsgs;
    mapping (uint256 => mapping(string => PortfolioHolding)) portfolioHoldingsData;
    uint256 loop = 0;
    int256 rebalanceMargin = int256(5) * FixidityLib.fixed1() / int256(100);
    int256 maxDrawdown = int256(5) * FixidityLib.fixed1() / int256(1000);

    constructor(){
        tickerNames = ["QQQ", "SPY"];
        tickerExists["QQQ"] = true;
        tickerExists["SPY"] = true;

        lastExecutedTimestamp = 0;
    }

    function inputRealTimeTickerData(string memory _tickerName, int256 bidPrice, int256 last, uint256 _timestamp) public {
        RealTimeTickerData memory tickerData = RealTimeTickerData(_tickerName, bidPrice, last);
        realTimeTickersData[_timestamp][_tickerName] = tickerData;

        if(loop == 0){
            maxDrawdownDodge[_tickerName] = false;
            benchmarkDrawdownPrice[_tickerName] = 0;
            liquidateTickerQty[_tickerName] = 0;
            maxStockPrice[_tickerName] = 0;
            liquidateTickerPrice[_tickerName] = 0;
        }

        if (!tickerExists[_tickerName]){
            tickerNames.push(_tickerName);
            tickerExists[_tickerName] = true;
            maxDrawdownDodge[_tickerName] = false;
            benchmarkDrawdownPrice[_tickerName] = 0;
            liquidateTickerQty[_tickerName] = 0;
            maxStockPrice[_tickerName] = 0;
            liquidateTickerPrice[_tickerName] = 0;
        }
    }

    function getRealTimeTickerData(string memory _tickerName, uint256 _timeStamp) public returns (string memory, int256, int256){
        RealTimeTickerData memory tickerData = realTimeTickersData[_timeStamp][_tickerName];
        return (tickerData.tickerName, tickerData.bidPrice, tickerData.last);
    }

    function inputMarginAccount(uint256 _timestamp, int256 FullInitMarginReq, int256 FullMainMarginReq) public {
        MarginAccount memory data = MarginAccount(FullInitMarginReq, FullMainMarginReq);
        marginAccountData[_timestamp] = data;
    }

    function getMarginAccount(uint256 timestamp) public view returns (int256,int256){
        MarginAccount memory data = marginAccountData[timestamp];
        return (data.FullInitMarginReq, data.FullMainMarginReq);
    }

    function inputTradingFunds(uint256 _timestamp, int256 AvailableFunds, int256 ExcessLiquidity, int256 BuyingPower, int256 Leverage, int256 EquityWithLoanValue) public {
        TradingFunds memory data = TradingFunds(AvailableFunds, ExcessLiquidity, BuyingPower, Leverage, EquityWithLoanValue);
        tradingFundsData[_timestamp] = data;
    }

    function getTradingFunds(uint256 timestamp) public view returns (int256,int256,int256,int256,int256){
        TradingFunds memory data = tradingFundsData[timestamp];
        return (data.AvailableFunds, data.ExcessLiquidity, data.BuyingPower, data.Leverage, data.EquityWithLoanValue);
    }

    function inputMktValue(uint256 _timestamp, int256 TotalCashValue, int256 NetDividend, int256 NetLiquidation, int256 UnrealizedPnL, int256 RealizedPnL, int256 GrossPositionValue) public {
        MktValue memory data = MktValue(TotalCashValue, NetDividend, NetLiquidation, UnrealizedPnL, RealizedPnL, GrossPositionValue);
        mktValueData[_timestamp] = data;
    }

    function getMktValue(uint256 _timestamp) public view returns (int256,int256,int256,int256,int256,int256){
        MktValue memory data = mktValueData[_timestamp];
        return (data.TotalCashValue, data.NetDividend, data.NetLiquidation, data.UnrealizedPnL, data.RealizedPnL, data.GrossPositionValue);
    }

    function updatePortfolioHoldingsData(uint256 timestamp, string memory _tickerName, int256 position, int256 marketPrice, int256 averageCost, int256 marketValue, int256 realizedPNL, int256 unrealizedPNL, int256 initMarginReq, int256 maintMarginReq, int256 costBasis) public {
        PortfolioHolding memory holding = PortfolioHolding(_tickerName, position, marketPrice, averageCost, marketValue, realizedPNL, unrealizedPNL, initMarginReq, maintMarginReq, costBasis);
        portfolioHoldingsData[timestamp][_tickerName] = holding;
    }

    function getPortfolioHolding(string memory _tickerName, uint256 _timestamp) public returns (string memory,int256,int256,int256,int256,int256,int256,int256,int256,int256){
        PortfolioHolding memory holding = portfolioHoldingsData[_timestamp][_tickerName];
        return (holding.tickerName, holding.position, holding.marketPrice, holding.averageCost, holding.marketValue, holding.realizedPNL, holding.unrealizedPNL, holding.initMarginReq, holding.maintMarginReq, holding.costBasis);
    }

    function getTickerTradeActionMsg(string memory tickerName, uint256 _timestamp) public returns (string memory, uint256, string memory, int256, int256, int256){
        TradeActionMsg memory tradeActionMsg = tradeActionMsgs[_timestamp][tickerName];
        return (tradeActionMsg.tickerName, tradeActionMsg.timestamp, tradeActionMsg.transactionType, tradeActionMsg.positionAction, tradeActionMsg.transactionTickerPrice, tradeActionMsg.transactionAmount);
    }

    function inputLoopWithTimestamps(uint256 _loop, uint256 _lastExecutedTimestamp) public {
        loop = _loop;
        executionTimestamps.push(_lastExecutedTimestamp);
    }

    function reinitializeContractState() public returns (string memory){
        string memory returnMsg = "";
        if (executionTimestamps.length == 0){
            returnMsg = "Contract state has not been initialized yet";
        }else{
            for (uint i=0; i < tickerNames.length; i++){
                delete tickerExists[tickerNames[i]];
                delete maxDrawdownDodge[tickerNames[i]];
                delete benchmarkDrawdownPrice[tickerNames[i]];
                delete liquidateTickerQty[tickerNames[i]];
                delete maxStockPrice[tickerNames[i]];
                delete liquidateTickerPrice[tickerNames[i]];
            }
            for (uint i=0; i < executionTimestamps.length; i++){
                for (uint j=0; j < tickerNames.length; j++){
                    delete realTimeTickersData[executionTimestamps[i]][tickerNames[j]];
                    delete portfolioHoldingsData[executionTimestamps[i]][tickerNames[j]];
                    delete tradeActionMsgs[executionTimestamps[i]][tickerNames[j]];
                }

                delete marginAccountData[executionTimestamps[i]];
                delete tradingFundsData[executionTimestamps[i]];
                delete mktValueData[executionTimestamps[i]];
                executionTimestamps[i] = 0;
            }
            lastExecutedTimestamp = 0;
            delete tickerNames;
            returnMsg = "Contract state has been reinitialized";
        }
        return returnMsg;
    }

    function test() public {
        string memory _tickerName = "AAPL";
        uint256 timestamp = 123456789;
//        tradeActionMsgs[timestamp][_tickerName].timestamp = timestamp;
//        tradeActionMsgs[timestamp][_tickerName].tickerName = _tickerName;
        TradeActionMsg memory tradeActionMsg = TradeActionMsg(_tickerName, 123456789, "buy", 0, 0, 0);
        tradeActionMsgs[timestamp][_tickerName] = tradeActionMsg;
    }

    function run(uint256 _timestamp) public{
        uint256 startGas = gasleft();

        string memory _tickerName = "AAPL";
        uint256 timestamp = 123456789;


        TradeActionMsg memory tradeActionMsg = TradeActionMsg(_tickerName, 123456789, "buy", 0, 0, 0);
        tradeActionMsgs[timestamp][_tickerName] = tradeActionMsg;
        uint256 gasUsed = startGas - gasleft();
        startGas = gasleft();

        executionTimestamps.push(_timestamp);
//        if(loop == 0){
//            int256 capitalForEachStock = FixidityLib.divide(mktValueData[_timestamp].TotalCashValue, int256(tickerNames.length) * FixidityLib.fixed1());
//
//            for (uint i=0; i < tickerNames.length; i++){
//                RealTimeTickerData memory tickerData = realTimeTickersData[_timestamp][tickerNames[i]];
//                string memory _tickerName = tickerData.tickerName;
////                int256 baseSharePurchase = FixidityLib.divide(capitalForEachStock,tickerData.last);
//                int256 _baseSharePurchase = capitalForEachStock/tickerData.last;
//                int256 _sharePurchase =  _baseSharePurchase * 3;
////                ActionMsg memory actionMsg = TradeAgent.placeBuyStockLimitOrderMsg(tickerName,sharePurchase,tickerPrice,_timestamp);
//                int256 _transactionAmount = _sharePurchase* tickerData.last;
//                tradeActionMsgs[_timestamp][_tickerName] = TradeActionMsg({tickerName:_tickerName, timestamp: _timestamp, transactionType: 'Buy', positionAction: _sharePurchase, transactionTickerPrice:tickerData.last, transactionAmount:_transactionAmount});
//            }
//        }
//        else{
//            int256 targetExLiq = FixidityLib.multiply(rebalanceMargin, mktValueData[_timestamp].GrossPositionValue);
//            for (uuint i=0; i < tickerNames.length; i++){
//                int256 tickerPrice = realTimeTickersData[_timestamp][tickerNames[i]].last;
//                string memory tickerName = tickerNames[i];
//                if (maxDrawdownDodge[tickerNames[i]]){
//                    if (checkBuyBack(tickerName, tickerPrice)){
//                        ActionMsg memory actionMsg = buyBackPosition(tickerName, tickerPrice,_timestamp);
//                        actionMsgs[_timestamp][tickerName] = actionMsg;
//                    }
//                }else{
//                    if(checkMaxDrawdownDodge(tickerName,tickerPrice)){
//                        ActionMsg memory actionMsg = liquidateTickerPosition(tickerName, tickerPrice ,_timestamp);
//                    }
//                    else{
//                        if (tradingFundsData[_timestamp].ExcessLiquidity > targetExLiq) {
//                            int256 exLiqDiff = FixidityLib.subtract(tradingFundsData[_timestamp].ExcessLiquidity, targetExLiq);
//                            int256 targetSharePurchase = FixidityLib.divide(exLiqDiff,tickerPrice);
//
//                            if (targetSharePurchase > 0){
//                                ActionMsg memory actionMsg = super.placeBuyStockLimitOrderMsg(tickerName,targetSharePurchase,tickerPrice,_timestamp);
//                                actionMsgs[_timestamp][tickerName] = actionMsg;
//                            }
//                        }
//                    }
//                }
//            }
//
//            for (uint i=0; i < tickerNames.length; i++){
//                int256 tickerPrice = realTimeTickersData[_timestamp][tickerNames[i]].last;
//                string memory tickerName = tickerNames[i];
//                if (maxDrawdownDodge[tickerName]){
//                    updateBenchmarkDrawdownPriceAfterDodge(tickerName, tickerPrice);
//                }
//                else{
//                    updateBenchmarkDrawdownPriceBeforeDodge(tickerName, tickerPrice);
//                }
//            }
//        }
        lastExecutedTimestamp = _timestamp;
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


    function checkBuyBack(string memory _tickerName, int256 realTimeTickerPrice)internal returns (bool){
//        range_price = self.benchmark_drawdown_price.get(ticker) * (1 + 0.01)
//        return ticker_price > range_price
        int256 rangePrice = benchmarkDrawdownPrice[_tickerName] /100*101;
        return realTimeTickerPrice > rangePrice;
    }

    function buyBackPosition(string memory _tickerName, int256 _tickerPrice, uint256 _timestamp) internal returns (TradeActionMsg memory) {
        TradeActionMsg memory tradeActionMsg;
        int256 targetSharePurchase = liquidateTickerQty[_tickerName];
        int256 purchaseAmount =  FixidityLib.multiply(targetSharePurchase, _tickerPrice);
        if (tradingFundsData[_timestamp].BuyingPower >= purchaseAmount){
            tradeActionMsg = super.placeBuyStockLimitOrderMsg(_tickerName,targetSharePurchase,_tickerPrice,_timestamp);
        }else{
            targetSharePurchase = tradingFundsData[_timestamp].BuyingPower / _tickerPrice;
            tradeActionMsg = super.placeBuyStockLimitOrderMsg(_tickerName,targetSharePurchase,_tickerPrice,_timestamp);
        }

        maxDrawdownDodge[_tickerName] = false;
        maxStockPrice[_tickerName] = _tickerPrice;

        return tradeActionMsg;
    }

    function getLastExecutedTimestamp() public view returns (uint256){
        return lastExecutedTimestamp;
    }

    function checkMaxDrawdownDodge(string memory tickerName, int256 tickerPrice) private returns (bool){
        return tickerPrice < benchmarkDrawdownPrice[tickerName];
    }

    function updateBenchmarkDrawdownPriceAfterDodge(string memory _tickerName, int256 _tickerPrice) private{
//        if real_time_ticker_price < self.benchmark_drawdown_price[ticker] * 0.7:
//            target_update_benchmark_drawdown_price = real_time_ticker_price + (
//                    (self.liq_sold_price_dict[ticker] - real_time_ticker_price) * 0.5)
//            if target_update_benchmark_drawdown_price < self.benchmark_drawdown_price[ticker]:
//                self.benchmark_drawdown_price[ticker] = target_update_benchmark_drawdown_price

        int256 benchmarkDrawdownFactor = int256(70) * FixidityLib.fixed1() / int256(100);
        if (_tickerPrice < FixidityLib.multiply(benchmarkDrawdownPrice[_tickerName] , benchmarkDrawdownFactor)){

            int256 tickerDiff = FixidityLib.subtract(liquidateTickerPrice[_tickerName],_tickerPrice);
            int256 targetUpdateBenchmarkDrawdownPrice = FixidityLib.add(_tickerPrice, tickerDiff) * 50 / 100;
            if (targetUpdateBenchmarkDrawdownPrice < benchmarkDrawdownPrice[_tickerName]){
                benchmarkDrawdownPrice[_tickerName] = targetUpdateBenchmarkDrawdownPrice;
            }
        }
    }

    function updateBenchmarkDrawdownPriceBeforeDodge(string memory _tickerName, int256 realTimeTickerPrice) private{
//        if (self.max_stock_price.get(ticker) == 0):  # if there is no data for the max and benchmark price
//            self.max_stock_price.update({ticker: real_time_ticker_price})
//            benchmark_price = real_time_ticker_price * (1 - self.max_drawdown_ratio)
//            self.benchmark_drawdown_price.update({ticker: benchmark_price})
//        elif real_time_ticker_price > self.max_stock_price.get(ticker):
//            self.max_stock_price.update({ticker: real_time_ticker_price})
//            benchmark_price = real_time_ticker_price * (1 - self.max_drawdown_ratio)
//            self.benchmark_drawdown_price.update({ticker: benchmark_price})

        if (maxStockPrice[_tickerName] == 0){
            maxStockPrice[_tickerName] = realTimeTickerPrice;
            int256 drawDownFactor = FixidityLib.subtract(FixidityLib.fixed1(), maxDrawdown);
            int256 tickerBenchmarkPrice = FixidityLib.multiply(maxStockPrice[_tickerName], drawDownFactor);
            benchmarkDrawdownPrice[_tickerName] = tickerBenchmarkPrice;
        }
        else if (realTimeTickerPrice > maxStockPrice[_tickerName]){
            maxStockPrice[_tickerName] = realTimeTickerPrice;
            int256 drawDownFactor = FixidityLib.subtract(FixidityLib.fixed1(), maxDrawdown);
            int256 tickerBenchmarkPrice = FixidityLib.multiply(maxStockPrice[_tickerName], drawDownFactor);
            benchmarkDrawdownPrice[_tickerName] = tickerBenchmarkPrice;
        }

    }

    function liquidateTickerPosition(string memory _tickerName, int256 tickerPrice, uint256 _timestamp) private returns (TradeActionMsg memory){
        PortfolioHolding memory holding = portfolioHoldingsData[_timestamp][_tickerName];
        RealTimeTickerData memory tickerData = realTimeTickersData[_timestamp][_tickerName];
        int256 currentPosition = holding.position;
        int256 noLeverageEquity = tradingFundsData[_timestamp].EquityWithLoanValue / int256(tickerNames.length);
        int256 noLeveragePosition = FixidityLib.divide(noLeverageEquity,tickerData.last);
        int256 targetLeveragePositionAtLiquidState = noLeveragePosition * 12 / 10;

        int256 targetSoldPosition = currentPosition - targetLeveragePositionAtLiquidState;
        TradeActionMsg memory tradeActionMsg = super.placeSellStockLimitOrderMsg(_tickerName,targetSoldPosition,tickerPrice, _timestamp);

        benchmarkDrawdownPrice[_tickerName] = tickerPrice;
        liquidateTickerQty[_tickerName] = targetSoldPosition;
        liquidateTickerPrice[_tickerName] = tickerPrice;
        maxDrawdownDodge[_tickerName] = true;

        return tradeActionMsg;
    }



}
