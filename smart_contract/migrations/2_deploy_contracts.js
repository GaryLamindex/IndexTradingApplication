const ChampCore = artifacts.require("./ChampCore.sol");
module.exports = function(deployer){
    deployer.deploy(ChampCore);
}