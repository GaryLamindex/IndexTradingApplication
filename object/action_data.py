from enum import Enum


class ActionMessage:
    def __init__(self):
        pass

    def __getattr__(self, item):
        return self.__dict__[item]

    def __getdict__(self):
        return self.__dict__


class ActionTuple:
    def __init__(self, timestamp, action_enum, args_dict):
        self.timestamp = timestamp
        self.action_enum = action_enum
        self.args_dict = args_dict

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __getdict__(self):
        return self.__dict__

    def __getattr__(self, item):
        return self.__dict__[item]


class IBAction(Enum):
    # market order
    BUY_MKT_ORDER = 1
    SELL_MKT_ORDER = 2
    CLOSE_POSITION = 3
    CLOSE_ALL = 4

    # limit order
    BUY_LMT_ORDER = 5
    SELL_LMT_ORDER = 6

    NO_ACTION = 7


class IBActionState(Enum):
    FAIL = 0
    SUCCESS = 1


class IBActionsTuple(ActionTuple):
    def __init__(self, timestamp, action_enum, args_dict):
        super().__init__(timestamp, action_enum, args_dict)


class IBActionMessage(ActionMessage):
    def __init__(self, state, timestamp, orderId, ticker, action, lmtPrice, totalQuantity, avgPrice, exchange,
                 commission):
        super().__init__()
        self.state = state
        self.timestamp = timestamp
        self.ticker = ticker
        self.action = action
        self.totalQuantity = totalQuantity
        self.avgPrice = avgPrice
        self.exchange = exchange
        self.commission = commission
        self.orderId = orderId
        self.lmtPrice = lmtPrice


class BinanceActionMessage(ActionMessage):
    def __init__(self, timestamp, ticker, side, price, quantity, realized_profit, action):
        super().__init__()
        self.timestamp = timestamp
        self.ticker = ticker
        self.side = side
        self.price = price
        self.quantity = quantity
        self.realized_profit = realized_profit
        self.action = action


class BinanceAction(Enum):
    # market order
    BUY_MKT_ORDER = 1
    SELL_MKT_ORDER = 2
    CLOSE_POSITION = 3
    CLOSE_ALL = 4

    # limit order
    BUY_LMT_ORDER = 5
    SELL_LMT_ORDER = 6


class BinanceActionState(Enum):
    REJECTED = 0
    FILLED = 1


class BinanceActionsTuple(ActionTuple):
    def __init__(self, timestamp, action_enum, args_dict):
        super().__init__(timestamp, action_enum, args_dict)

