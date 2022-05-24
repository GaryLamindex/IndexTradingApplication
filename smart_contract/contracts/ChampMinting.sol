pragma solidity ^0.8.0;

import "./ChampAuction.sol";


/// @title all functions related to creating kittens
contract ChampMinting is ChampAuction {

    // Limits the number of cats the contract owner can ever create.=
    uint256 public constant GEN0_CREATION_LIMIT = 45000;

    // Constants for gen0 auctions.
    uint256 public constant GEN0_STARTING_PRICE = 10 * 1e15;
    uint256 public constant GEN0_AUCTION_DURATION = 1 days;

    // Counts the number of cats the contract owner has created.
    uint256 public gen0CreatedCount;

    /// @dev Creates a new gen0 kitty with the given genes and
    ///  creates an auction for it.
    function createGen0Auction(string memory _philosophyIpfsHash, address _trader) external onlyCOO{
        require(gen0CreatedCount < GEN0_CREATION_LIMIT);
        uint256 newChampId = _createChamp(_philosophyIpfsHash, address(this), _trader);
        _approve(newChampId, address(saleAuctionContract) );

        saleAuctionContract.createAuction(
            newChampId,
            _computeNextGen0Price(),
            0,
            GEN0_AUCTION_DURATION,
            address(this)
        );

        gen0CreatedCount++;
    }



    /// @dev Computes the next gen0 auction starting price, given
    ///  the average of the past 5 prices + 50%.
    function _computeNextGen0Price() internal view returns (uint256) {
        uint256 avePrice = saleAuctionContract.averageGen0SalePrice();

        // Sanity check to ensure we don't overflow arithmetic
        require(avePrice == uint256(uint128(avePrice)));

        uint256 nextPrice = avePrice + (avePrice / 2);

        // We never auction for less than starting price
        if (nextPrice < GEN0_STARTING_PRICE) {
            nextPrice = GEN0_STARTING_PRICE;
        }

        return nextPrice;
    }
}
