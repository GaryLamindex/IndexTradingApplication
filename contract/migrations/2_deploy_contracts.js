

const RebalanceMarginWifMaxDrawdownControl = artifacts.require("RebalanceMarginWifMaxDrawdownControl");

module.exports = function(deployer) {
    // Demo is the contract's name
    deployer.deploy(RebalanceMarginWifMaxDrawdownControl);
};