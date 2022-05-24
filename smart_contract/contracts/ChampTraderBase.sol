pragma solidity ^0.8.0;
import "./ChampTraderAccessControl.sol";
import "./SaleClockAuction.sol";

contract ChampTraderBase is ChampTraderAccessControl {
    /// @dev Transfer event as defined in current draft of ERC721. Emitted every time a kitten
    ///  ownership is assigned, including births.
    event Birth(address indexed owner, address indexed trader, uint256 champId, string philosophyIpfsHash);
    struct ChampTrader {
        string tradeLog;
        string philosophyIpfsHash;
        uint64 birthTime;
        uint256 champId;
        uint32 totalTransactionQty;
        address trader;
    }

    /*** STORAGE ***/

    ChampTrader[] champTraders;


    /// @dev A mapping from cat IDs to the address that owns them. All cats have
    ///  some valid owner address, even gen0 cats are created with a non-zero owner.
    mapping (uint256 => address) public champIndexToOwner;

    // @dev A mapping from owner address to count of tokens that address owns.
    //  Used internally inside balanceOf() to resolve ownership count.
    mapping (address => uint256) ownershipTokenCount;

    /// @dev A mapping from KittyIDs to an address that has been approved to call
    ///  transferFrom(). Each Kitty can only have one approved address for transfer
    ///  at any time. A zero value means no approval is outstanding.
    mapping (uint256 => address) public champIndexToApproved;

    mapping (uint256 => address) public champIndexToTrader;

    /// @dev The address of the ClockAuction contract that handles sales of Kitties. This
    ///  same contract handles both peer-to-peer sales as well as the gen0 sales which are
    ///  initiated every 15 minutes.
    SaleClockAuction public saleAuctionContract;

    /// @dev Assigns ownership of a specific Kitty to an address.
    function _transfer(address _from, address _to, uint256 _tokenId) internal {
        // Since the number of kittens is capped to 2^32 we can't overflow this
        ownershipTokenCount[_to]++;
        // transfer ownership
        champIndexToOwner[_tokenId] = _to;
        // When creating new kittens _from is 0x0, but we can't account that address.
        if (_from != address(0)) {
            ownershipTokenCount[_from]--;
            // clear any previously approved ownership exchange
            delete champIndexToApproved[_tokenId];
        }

    }

    /// @dev An internal method that creates a new kitty and stores it. This
    ///  method doesn't do any checking and should only be called when the
    ///  input data is known to be valid. Will generate both a Birth event
    ///  and a Transfer event.
    /// @param _owner The inital owner of this cat, must be non-zero (except for the unKitty, ID 0)
    function _createChamp(string memory _philosophyIpfsHash,address _owner, address _trader) internal returns (uint256)
    {
        uint256 newChampId = champTraders.length;

        ChampTrader memory _champ = ChampTrader({
            champId:newChampId,
            tradeLog:"",
            philosophyIpfsHash: _philosophyIpfsHash,
            birthTime: uint64(block.timestamp),
            trader: _trader,
            totalTransactionQty:0
        });
        champTraders.push(_champ);

        // emit the birth event
        emit Birth(_owner,_trader, newChampId,_champ.philosophyIpfsHash);

        // This will assign ownership, and also emit the Transfer event as
        // per ERC721 draft
        _transfer(address(0), _owner, newChampId);

        return newChampId;
    }

}
