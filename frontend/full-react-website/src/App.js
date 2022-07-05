import logo from './logo.svg';
import './App.css';
import {useState} from "react";
import NavBar from "./NavBar";
import React from "react";
import { ethers } from "ethers";
import algoNFT from "./contracts/RebalanceMarginWifMaxDrawdownControl.json";

const algoNFTAddress = "0x13FcD9aA90eD9724051f006F4F577B0cE0A4C35a";


function App() {
    const [accounts, setAccounts] = useState([]);
    async function getTradeSignal(){
        if (window.ethereum) {
            const accounts = await window.ethereum.request({ method: "eth_requestAccounts" });
        }
        setAccounts(accounts);

        const provider = new ethers.providers.Web3Provider(window.ethereum);
        const signer = provider.getSigner();
        const contract = new ethers.Contract(algoNFTAddress, algoNFT.abi, signer);

        try{
            console.log("hi ");
            // //array of structs of function [RealTimeTickerData[] memory _tickerData, uint256 _timeStamp, PortfolioData memory _portfolioData, PortfolioHolding[] memory _portfolioHoldings]
            // const timeStamp = Date.now();
            // const AccountCode = "A";
            // const Currency = "USD";
            // const ExchangeRate = 1;
            // const FullInitMarginReq = 0;
            // const FullMainMarginReq = 0;
            // const AvailableFunds = 0;
            // const ExcessLiquidity = 0;
            // const BuyingPower = 0;
            // const Leverage = 0;
            // const EquityWithLoanValue = 0;
            // const TotalCashValue = 0;
            // const NetDividend = 0;
            // const NetLiquidation = 0;
            // const UnrealizedPnL = 0;
            // const RealizedPnL = 0;
            // const GrossPositionValue = 0;
            //
            // const portfolioData = [AccountCode, Currency, ExchangeRate, FullInitMarginReq, FullMainMarginReq,
            //     AvailableFunds, ExcessLiquidity, BuyingPower, Leverage, EquityWithLoanValue, TotalCashValue, NetDividend,
            //     NetLiquidation, UnrealizedPnL, RealizedPnL, GrossPositionValue];
            //
            // const portfolioHoldings = [["SPY",0,0,0,0,0,0,0,0,0], ["SPY",0,0,0,0,0,0,0,0,0]];
            const tickerData = [["SPY", 0, 0], ["QQQ", 1, 1]];
            // const response = await contract.run(tickerData, timeStamp, portfolioData, portfolioHoldings);
            const response = await contract.test(tickerData);
            console.log("response: ", response);
        }catch (error){
            console.log("error: ", error);
        }
    }

    return (
    <div className="App">
      <NavBar accounts={accounts} setAccounts = {setAccounts}/>

      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.js</code> and save to reload.
        </p>

        <button onClick={getTradeSignal}>Get Trade Signal</button>
      </header>
    </div>
    );
}

export default App;
