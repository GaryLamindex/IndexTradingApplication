pragma solidity ^0.8.0;

contract RebalanceMarginWifMaxDrawdownControl {
    mapping (address => mapping(uint256 => RealTimeTickerData)) realTimeTickersData;

    mapping(address => mapping(uint256 => MarginAccount)) marginAccountData;
    mapping(address => mapping(uint256 => TradingFunds)) tradingFundsData;
    mapping(address => mapping(uint256 => MktValue)) mktValueData;

    mapping (address => mapping(uint256 => mapping(string => TradeActionMsg))) tradeActionMsgs;
    mapping (address => mapping(uint256 => mapping(string => PortfolioHolding))) portfolioHoldings;

    mapping (address => string[]) tickerNames;
    mapping (address => mapping(string => bool)) tickerExists;
    mapping (address => uint256[]) executionTimestamps;
    mapping (address => mapping(uint256 => bool)) executionTimestampsExists;

    function inputRealTimeTickerData(address addr, string memory _tickerName, int256 bidPrice, int256 last, uint256 _timestamp) public {
        RealTimeTickerData memory tickerData = RealTimeTickerData(_tickerName, bidPrice, last);
        realTimeTickersData[addr][_timestamp][_tickerName] = tickerData;

        if (!tickerExists[_tickerName]){
            tickerNames[addr].push(_tickerName);
            tickerExists[addr][_tickerName] = true;
        }
    }

        function getRealTimeTickerData(address addr, string memory _tickerName, uint256 _timeStamp) public returns (string memory, int256, int256){
        RealTimeTickerData memory tickerData = realTimeTickersData[addr][_timeStamp][_tickerName];
        return (tickerData.tickerName, tickerData.bidPrice, tickerData.last);
    }

    function inputMarginAccount(address addr, uint256 _timestamp, int256 FullInitMarginReq, int256 FullMainMarginReq) public {
        MarginAccount memory data = MarginAccount(FullInitMarginReq, FullMainMarginReq);
        marginAccountData[addr][_timestamp] = data;
    }

    function getMarginAccount(address addr, uint256 timestamp) public view returns (int256,int256){
        MarginAccount memory data = marginAccountData[addr][timestamp];
        return (data.FullInitMarginReq, data.FullMainMarginReq);
    }

    function inputTradingFunds(address addr, uint256 _timestamp, int256 AvailableFunds, int256 ExcessLiquidity, int256 BuyingPower, int256 Leverage, int256 EquityWithLoanValue) public {
        TradingFunds memory data = TradingFunds(AvailableFunds, ExcessLiquidity, BuyingPower, Leverage, EquityWithLoanValue);
        tradingFundsData[addr][_timestamp] = data;
    }

    function getTradingFunds(address addr, uint256 timestamp) public view returns (int256,int256,int256,int256,int256){
        TradingFunds memory data = tradingFundsData[addr][timestamp];
        return (data.AvailableFunds, data.ExcessLiquidity, data.BuyingPower, data.Leverage, data.EquityWithLoanValue);
    }

    function inputMktValue(address addr, uint256 _timestamp, int256 TotalCashValue, int256 NetDividend, int256 NetLiquidation, int256 UnrealizedPnL, int256 RealizedPnL, int256 GrossPositionValue) public {
        MktValue memory data = MktValue(TotalCashValue, NetDividend, NetLiquidation, UnrealizedPnL, RealizedPnL, GrossPositionValue);
        mktValueData[addr][_timestamp] = data;
    }

    function getMktValue(address addr, uint256 _timestamp) public view returns (int256,int256,int256,int256,int256,int256){
        MktValue memory data = mktValueData[addr][_timestamp];
        return (data.TotalCashValue, data.NetDividend, data.NetLiquidation, data.UnrealizedPnL, data.RealizedPnL, data.GrossPositionValue);
    }

    function updatePortfolioHoldingsData(address addr, uint256 timestamp, string memory _tickerName, int256 position, int256 marketPrice, int256 averageCost, int256 marketValue, int256 realizedPNL, int256 unrealizedPNL, int256 initMarginReq, int256 maintMarginReq, int256 costBasis) public {
        PortfolioHolding memory holding = PortfolioHolding(_tickerName, position, marketPrice, averageCost, marketValue, realizedPNL, unrealizedPNL, initMarginReq, maintMarginReq, costBasis);
        portfolioHoldings[addr][timestamp][_tickerName] = holding;
    }

    function getPortfolioHolding(address addr, string memory _tickerName, uint256 _timestamp) public returns (string memory,int256,int256,int256,int256,int256,int256,int256,int256,int256){
        PortfolioHolding memory holding = portfolioHoldings[addr][_timestamp][_tickerName];
        return (holding.tickerName, holding.position, holding.marketPrice, holding.averageCost, holding.marketValue, holding.realizedPNL, holding.unrealizedPNL, holding.initMarginReq, holding.maintMarginReq, holding.costBasis);
    }

    function getTickerTradeActionMsg(address addr, string memory tickerName, uint256 _timestamp) public returns (string memory, uint256, string memory, int256, int256, int256){
        TradeActionMsg memory tradeActionMsg = tradeActionMsgs[addr][_timestamp][tickerName];
        return (tradeActionMsg.tickerName, tradeActionMsg.timestamp, tradeActionMsg.transactionType, tradeActionMsg.positionAction, tradeActionMsg.transactionTickerPrice, tradeActionMsg.transactionAmount);
    }

    function reinitializeUserContractState(address addr) public returns (string memory){
        string memory returnMsg = "";
        if (executionTimestamps[addr].length == 0){
            returnMsg = "Contract state has not been initialized yet";
        }else{
            for (uint i=0; i < tickerNames.length; i++){
                delete tickerExists[addr][tickerNames[i]];
            }
            for (uint i=0; i < executionTimestamps[addr].length; i++){
                for (uint j=0; j < tickerNames.length; j++){
                    delete realTimeTickersData[addr][executionTimestamps[i]][tickerNames[j]];
                    delete portfolioHoldings[addr][executionTimestamps[i]][tickerNames[j]];
                    delete tradeActionMsgs[addr][executionTimestamps[i]][tickerNames[j]];
                }

                delete marginAccountData[addr][executionTimestamps[i]];
                delete tradingFundsData[addr][executionTimestamps[i]];
                delete mktValueData[addr][executionTimestamps[i]];
                executionTimestamps[addr][i] = 0;
            }
            delete tickerNames[addr];
            returnMsg = "Contract state has been reinitialized";
        }
        return returnMsg;
    }
}
