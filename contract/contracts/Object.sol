pragma solidity ^0.8.13;

contract Object {

    struct RealTimeTickerData{
        string tickerName;
        int256 bidPrice;
        int256 last;
    }

    struct ActionMsg{
        string tickerName;
        uint256 timestamp;
        string transactionType;
        int256 positionAction;
        int256 transactionTickerPrice;
        int256 transactionAmount;
    }

    struct MarginAccount{
        int256 FullInitMarginReq;
        int256 FullMainMarginReq;
    }

    struct TradingFunds{
        int256 AvailableFunds;
        int256 ExcessLiquidity;
        int256 BuyingPower;
        int256 Leverage;
        int256 EquityWithLoanValue;
    }

    struct MktValue{
        int256 TotalCashValue;
        int256 NetDividend;
        int256 NetLiquidation;
        int256 UnrealizedPnL;
        int256 RealizedPnL;
        int256 GrossPositionValue;
    }

    struct PortfolioData{

        //margin_acc
        int256 FullInitMarginReq;
        int256 FullMainMarginReq;

        //trading_funds
        int256 AvailableFunds;
        int256 ExcessLiquidity;
        int256 BuyingPower;
        int256 Leverage;
        int256 EquityWithLoanValue;

        //mkt_value
        int256 TotalCashValue;
        int256 NetDividend;
        int256 NetLiquidation;
        int256 UnrealizedPnL;
        int256 RealizedPnL;
        int256 GrossPositionValue;


    }

    //ticker portfolio
    struct PortfolioHolding{
        string tickerName;
        int256 position;
        int256 marketPrice;
        int256 averageCost;
        int256 marketValue;
        int256 realizedPNL;
        int256 unrealizedPNL;
        int256 initMarginReq;
        int256 maintMarginReq;
        int256 costBasis;
    }


}
