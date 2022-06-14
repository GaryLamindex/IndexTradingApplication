pragma solidity ^0.8.13;

contract Object {

    struct RealTimeTickerData{
        string tickerName;
        uint256 bidPrice;
        uint256 last;
    }

    struct ActionMsg{
        string tickerName;
        uint256 timestamp;
        string transactionType;
        int256 positionAction;
        int256 transactionTickerPrice;
        int256 transactionAmount;
    }

    struct PortfolioData{
        //accData
        string AccountCode;
        string Currency;
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
    }
}
