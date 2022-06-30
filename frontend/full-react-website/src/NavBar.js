import React from "react";

const NavBar = ({ accounts, setAccounts }) => {
    const isConnected = Boolean(accounts[0]);
    async function connectAccount() {
        if (window.ethereum) {
            const accounts = await window.ethereum.request({ method: "eth_requestAccounts" });
        }
        setAccounts(accounts);
    }

    return (
        <div>
            <div>Facebook</div>
            <div>Twitter</div>
            <div>Instagram</div>
    
            <div>About</div>
            <div>Mint</div>
            <div>Team</div>

            {isConnected ? (
                <p>Connected</p>
            ) : (
                <button onClick={connectAccount}>Connect</button>
            )}
        </div>
        );
};

export default NavBar;