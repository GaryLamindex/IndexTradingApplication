from socket import AI_PASSIVE
import get_stock_data_engine

# there is a slight difference between the df from local and online
# the timestamp of the local one is a number
# the timestamp of the online one seems to be a timestamp object
class get_stock_data_API:
    aws = False # a boolean value storing whether the use want to fetch online or locally
    get_stock_data_engine = None
    ticket = ""

    # initialize the API with a specified 5 days df
    def __init__(self,aws):
        if aws:
            self.get_stock_data_engine = get_stock_data_engine.online_engine()
        else:
            self.get_stock_data_engine = get_stock_data_engine.local_engine()

    # in case you want to perform some other retrivements
    # n is the number of days looking backward
    def get_stock_data_by_timestamp(self, ticker, timestamp, n):
                return self.get_stock_data_engine.get_n_days_data(self.ticket,timestamp,n)

    def get_stock_data_by_range(self,range):
        return self.get_stock_data_engine.get_data_by_range(self.ticket,range)

def main():
    my_API = get_stock_data_API("QQQ",True)
    print(my_API.get_stock_data_by_timestamp(1630419240,5))
    my_API.change_ticket("SPY")
    print(my_API.get_stock_data_by_timestamp(1630419240,5))
    my_API.change_ticket("QQQ")
    print(my_API.get_stock_data_by_range([1629811920,1629811980]))

    my_API_2 = get_stock_data_API("QQQ",False)
    print(my_API_2.get_stock_data_by_timestamp(1630419240,5))
    my_API_2.change_ticket("SPY")
    print(my_API_2.get_stock_data_by_timestamp(1630419240,5))
    my_API_2.change_ticket("QQQ")
    print(my_API_2.get_stock_data_by_range([1629811920,1629811980]))

if __name__ == "__main__":
    main()