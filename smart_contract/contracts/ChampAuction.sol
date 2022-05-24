pragma solidity ^0.8.0;

import "./ChampLogging.sol";
import "./SaleClockAuction.sol";
/// @title Handles creating auctions for sale and siring of kitties.
///  This wrapper of ReverseAuction exists only so that users can create
///  auctions with only one transaction.
contract ChampAuction is ChampLogging {

    // @notice The auction contract variables are defined in KittyBase to allow
    //  us to refer to them in KittyOwnership to prevent accidental transfers.
    // `saleAuction` refers to the auction for gen0 and p2p sale of kitties.
    // `siringAuction` refers to the auction for siring rights of kitties.

    /// @dev Sets the reference to the sale auction.
    /// @param _address - Address of sale contract.
    function setSaleAuctionAddress(address _address) external onlyCEO {
        SaleClockAuction candidateContract = SaleClockAuction(_address);

        // NOTE: verify that a contract is what we expect - https://github.com/Lunyr/crowdsale-contracts/blob/cfadd15986c30521d8ba7d5b6f57b4fefcc7ac38/contracts/LunyrToken.sol#L117
        require(candidateContract.isSaleClockAuction());

        // Set the new contract address
        saleAuctionContract = candidateContract;
    }


    /// @dev Put a kitty up for auction.
    ///  Does some ownership trickery to create auctions in one tx.
    function createSaleAuction(uint256 _champId,uint256 _startingPrice,uint256 _endingPrice,uint256 _duration) external whenNotPaused{
        // Auction contract checks input sizes
        // If kitty is already on any auction, this will throw
        // because it will be owned by the auction contract.
        require(_owns(msg.sender, _champId));
        // Ensure the kitty is not pregnant to prevent the auction
        // contract accidentally receiving ownership of the child.
        // NOTE: the kitty IS allowed to be in a cooldown.
        require(isActive(_champId));
        _approve(_champId, address(saleAuctionContract));
        // Sale auction throws if inputs are invalid and clears
        // transfer and sire approval after escrowing the kitty.
        saleAuctionContract.createAuction(
            _champId,
            _startingPrice,
            _endingPrice,
            _duration,
            msg.sender
        );
    }

    /// @dev Transfers the balance of the sale auction contract
    /// to the KittyCore contract. We use two-step withdrawal to
    /// prevent two transfer calls in the auction bid function.
    function withdrawAuctionBalances() external onlyCLevel {
        saleAuctionContract.withdrawBalance();
    }
}
