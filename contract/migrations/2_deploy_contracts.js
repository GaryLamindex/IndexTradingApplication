const FixidityLib = artifacts.require("utils/FixidityLib");
const DateUtils = artifacts.require("utils/DateUtils");
const StringLib = artifacts.require("utils/StringLib");
const StringUtils = artifacts.require("utils/StringUtils");
const RebalanceMarginWifMaxDrawdownControl = artifacts.require("RebalanceMarginWifMaxDrawdownControl");

async function doDeploy(deployer){
    await deployer.deploy(FixidityLib);
    await deployer.deploy(DateUtils);
    await deployer.deploy(StringLib);
    
    await deployer.link(StringLib, StringUtils);
    await deployer.deploy(StringUtils);

    await deployer.link(FixidityLib, RebalanceMarginWifMaxDrawdownControl);
    await deployer.link(DateUtils, RebalanceMarginWifMaxDrawdownControl);
    await deployer.link(StringUtils, RebalanceMarginWifMaxDrawdownControl);
    await deployer.deploy(RebalanceMarginWifMaxDrawdownControl);
}


module.exports = function(deployer) {
    // Demo is the contract's name
    deployer.then(async() =>{
       await doDeploy(deployer)
    });
};