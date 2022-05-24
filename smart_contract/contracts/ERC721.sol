pragma solidity ^0.8.0;

/// @author Dieter Shirley <dete@axiomzen.co> (https://github.com/dete)
abstract contract ERC721 {
    // Required methods
    function totalSupply() virtual public view returns (uint256 total);
    function balanceOf(address _owner) virtual public view returns (uint256 balance);
    function ownerOf(uint256 _tokenId) virtual external view returns (address owner);
    function approve(address _to, uint256 _tokenId) virtual external;
    function transfer(address _to, uint256 _tokenId) virtual external;
    function transferFrom(address _from, address _to, uint256 _tokenId) virtual external;

    // Events
    event Transfer(address from, address to, uint256 tokenId);
    event Approval(address owner, address approved, uint256 tokenId);

    // Optional
    // function name() public view returns (string name);
    // function symbol() public view returns (string symbol);
    function tokensOfOwner(address _owner) virtual  external view returns (uint256[] memory tokenIds);
    function tokenMetadata(uint256 _tokenId, string memory _preferredTransport) virtual public view returns (string memory infoUrl);

    // ERC-165 Compatibility (https://github.com/ethereum/EIPs/issues/165)
    function supportsInterface(bytes4 _interfaceID) virtual external view returns (bool);
}


