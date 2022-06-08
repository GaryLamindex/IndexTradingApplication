pragma solidity ^0.8.0;

contract Object {

    struct RealTimeTickerData{
        string tickerName;
        uint256 bidPrice;
        uint256 last;
    }

    struct PortfolioData{
        //accData
        string AccountCode;
        string Currency;
        string HKD;
        string ExchangeRate;

        //margin_acc
        uint256 FullInitMarginReq;
        uint256 FullMainMarginReq;

        //trading_funds
        uint256 AvailableFunds;
        uint256 ExcessLiquidity;
        uint256 BuyingPower;
        uint256 Leverage;
        uint256 EquityWithLoanValue;

        //mkt_value
        uint256 TotalCashValue;
        uint256 NetDividend;
        uint256 NetLiquidation;
        uint256 UnrealizedPnL;
        uint256 RealizedPnL;
        uint256 GrossPositionValue;

        //Ticker portfolio
        mapping(string => uint256) tickerPortfolio;
    }
}
