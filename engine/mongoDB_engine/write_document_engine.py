from engine.mongoDB_engine.mongodb_engine import mongodb_engine


class Write_Mongodb:

    mongo = None

    def __init__(self):
        self.mongo = mongodb_engine()

    def write_Strategies(self, dict_list):
        for item in dict_list:
            if self.mongo.db['Strategies'].count_documents({'name': item['name']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("Strategies", item)

    def write_Clients(self, dict_list):
        for item in dict_list:
            if self.mongo.db['Clients'].count_documents({'name': item['name']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("Clients", item)

    def write_Traders(self, dict_list):
        for item in dict_list:
            if self.mongo.db['Traders'].count_documents({'name': item['name']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("Traders", item)

    def write_Transactions(self, dict_list):
        for item in dict_list:
            if self.mongo.db['Transactions'].count_documents({'-id': item['_id']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("Transactions", item)

    def write_ETFs(self, dict_list):
        for item in dict_list:
            if self.mongo.db['ETFs'].count_documents({'name': item['name']}) > 0:
                print("document already exist")
            else:
                self.mongo.write_mongodb_dict_list("ETFs", item)

    def write_ETF_raw_data(self, dict_list):
        self.mongo.write_mongodb_dict_list("ETF_raw_data", dict_list)

    def write_Performance(self, dict_list, _name):
        if self.mongo.db['Performance'].count_documents({'name': _name['name']}) > 0:
            print("document already exist")
        else:
            for item in dict_list:
                self.mongo.write_mongodb_dict_list("Performance", item)


def main():
    # w = Write_Mongodb()
    #
    # client_data = {"name": ["Ivy", "Peter", "Percy"], "VIP":["Premium", "basic","Gold+"], "Followed":[[3,5],[6,5,2,1],[0,5,8,9,45]]}
    # client_df = pd.DataFrame(data=client_data)
    # dict = client_df.to_dict(orient='records')
    # w.write_Clients(dict)
    #
    # Trader_data = {"name": ["V. Poor", "N. Win", "Not Warrent Buffet"], "VIP": ["Premium", "basic", "Gold+"],
    #                "return":[0.8, 3.4, 7.2],
    #                "rating": [3.4, 7, 8.4]}
    # Trader_df = pd.DataFrame(data=Trader_data)
    # dict = Trader_df.to_dict(orient='records')
    # w.write_Traders(dict)
    #
    # strategies_data = {"name": ["buffet50", "ark kiss", "poor"], "Return":[5,7,0.2]}
    # strategies_df = pd.DataFrame(data=strategies_data)
    # _dict = strategies_df.to_dict(orient='records')
    # w.write_Strategies(_dict)
    #
    return

if __name__ == "__main__":
    main()