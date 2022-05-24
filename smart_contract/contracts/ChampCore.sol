pragma solidity ^0.8.0;

import "./ChampMinting.sol";

/// @title CryptoKitties: Collectible, breedable, and oh-so-adorable cats on the Ethereum blockchain.
/// @author Axiom Zen (https://www.axiomzen.co)
/// @dev The main CryptoKitties contract, keeps track of kittens so they don't wander around and get lost.
contract ChampCore is ChampMinting {

    // Set in case the core contract is broken and an upgrade is required
    address public newContractAddress;

//    /// @notice Creates the main CryptoKitties smart contract instance.
//    function ChampCore() public {
//        // Starts paused.
//        paused = true;
//
//        // the creator of the contract is the initial CEO
//        ceoAddress = msg.sender;
//
//        // the creator of the contract is also the initial COO
//        cooAddress = msg.sender;
//
//        // start with the mythical kitten 0 - so we don't have generation-0 parent issues
////        _createChamp(0, 0, 0, uint256(-1), address(0));
//        _createChamp("", address(0), address(0));
//    }
    constructor() public {
        // Starts paused.
        paused = true;

        // the creator of the contract is the initial CEO
        ceoAddress = msg.sender;

        // the creator of the contract is also the initial COO
        cooAddress = msg.sender;

        // start with the mythical kitten 0 - so we don't have generation-0 parent issues
//        _createChamp(0, 0, 0, uint256(-1), address(0));
        _createChamp("", address(0), address(0));
    }

    /// @dev Used to mark the smart contract as upgraded, in case there is a serious
    ///  breaking bug. This method does nothing but keep track of the new contract and
    ///  emit a message indicating that the new address is set. It's up to clients of this
    ///  contract to update to the new contract address in that case. (This contract will
    ///  be paused indefinitely if such an upgrade takes place.)
    /// @param _v2Address new address
    function setNewAddress(address _v2Address) external onlyCEO whenPaused {
        // See README.md for updgrade plan
        newContractAddress = _v2Address;
        emit ContractUpgrade(_v2Address);
    }

    /// @notice No tipping!
    /// @dev Reject all Ether from being sent here, unless it's from one of the
    ///  two auction contracts. (Hopefully, we can prevent user accidents.)
    fallback() external payable {
        require(msg.sender == address(saleAuctionContract));
    }

    /// @notice Returns all the relevant information about a specific kitty.
    function getChamp(uint32 _champId) external view returns (string memory tradeLog, uint64 birthTime, string memory philosophy, uint32 totalTransactionQty, address trader){

        ChampTrader storage champ = champTraders[_champId];
        tradeLog = string(champ.tradeLog);
        birthTime = uint64(champ.birthTime);
        philosophy = string(champ.philosophyIpfsHash);
        totalTransactionQty = uint32(champ.totalTransactionQty);
        trader = address(champ.trader);
    }

    /// @dev Override unpause so it requires all external contract addresses
    ///  to be set before contract can be unpaused. Also, we can't have
    ///  newContractAddress set either, because then the contract was upgraded.
    /// @notice This is public rather than external so we can call super.unpause
    ///  without using an expensive CALL.
    function unpause() public override onlyCEO whenPaused {
        require(address(saleAuctionContract) != address(0));
        require(newContractAddress == address(0));

        // Actually unpause the contract.
        super.unpause();
    }

    // @dev Allows the CFO to capture the balance available to the contract.
    function withdrawBalance() external onlyCFO{
        uint256 balance = address(this).balance;
        // Subtract all the currently pregnant kittens we have, plus 1 of margin.
//        uint256 subtractFees = (pregnantKitties + 1) * autoLogFee;
        uint256 subtractFees = (activeChamps + 1) * autoLogFee;

        if (balance > subtractFees) {
            cfoAddress.transfer(balance - subtractFees);
        }
    }
}