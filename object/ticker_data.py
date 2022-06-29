class IBTickerData:
    def __init__(self, ticker, position, marketPrice, averageCost, marketValue, costBasis, unrealizedPNL, realizedPNL, initMarginReq, maintMarginReq):
        self.ticker = ticker
        self.position = position
        self.marketPrice = marketPrice
        self.averageCost = averageCost
        self.marketValue = marketValue
        self.costBasis = costBasis
        self.unrealizedPNL = unrealizedPNL
        self.realizedPNL = realizedPNL
        self.initMarginReq = initMarginReq
        self.maintMarginReq = maintMarginReq

    def __getitem__(self, item):
        return self.__dict__[item]

    def __getdict__(self):
        return self.__dict__

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    # def get_dict(self):
    #     return self.__dict__

