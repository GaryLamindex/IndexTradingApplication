import {useState} from "react";
import {ethers, BigNumber} from "ethers";
import roboPunksNFT from "./contracts/RoboPunksNFT.json";

const roboPunksNFTAddress = "0x8f8b8f8b8f8b8f8b8f8b8f8b8f8b8f8b8f8b8f8";

const MainMint = ({accounts, setAccounts}) => {
    const [mintAmount, setMintAmount] = useState(1);
    const isConnected = Boolean(accounts[0])

    async function handleMint(){
        if (!window.ethereum) {
            return <div>Please install MetaMask</div>;
        } else {
            const provider = new ethers.provider.Web3Provider(window.ethereum);
            const signer = provider.getSigner();
            const contract = new ethers.CONTRACT(roboPunksNFTAddress, roboPunksNFT.abi, signer);
            try {
                const response = await contract.mint(BigNumber.from(mintAmount));
                value:  ethers.utils.parseEther((0.02 * mintAmount).toString());
                console.log("response: ", response);
            } catch (error) {
                console.log("error: ", error);
            }
        }
    }
}

    const handleDecrement =() => {
        if(mintAmount <= 1)
            return;
        setMintAmount(mintAmount - 1);
    }

    const handleIncrement =() => {
        if (mintAmount >= 100)
            return;
        setMintAmount(mintAmount + 1);
    }

    return(
        <div>
            <h1>RoboPunk</h1>
            <p>test content</p>
            {isConnected?(
                <div>
                    <div>
                        <button onClick={handleDecrement}>-</button>
                        <input type="number" value={mintAmount}/>
                        <button onClick={handleIncrement}>+</button>
                    </div>
                    <button onClick ={handleMint}>Mint Now</button>
                </div>
            ) : (
                <p>You must be connected to Mint.</p>
            )}
        </div>
    );
}

export default MainMint;
