from pymongo import MongoClient
import certifi

class mongodb:
    db = None
    conn = None

    def __init__(self):
        # initialize connection to mongodb
        # and choose the correct database
        try:
            self.conn = MongoClient('mongodb+srv://nft:nft123@nft.qrqri.mongodb.net/?retryWrites=true&w=majority', tlsCAFile = certifi.where())
            self.db = self.conn['rainydrop']
            print(f"Successful connection to database: {self.db.name}")
        except:
            print("WARNING: Could not connect to MongoDB")

    def get_mongodb_all_json(self, _collection):
        # return a cursor object
        if _collection in self.db.list_collection_names():
            coll = self.db[_collection]
        else:
            print("WARNING: This collection doesn't exist")
            exit(1)

        result = coll.find()

        return result

    def write_mongodb_json(self, _collection, json):
        # json is a json type data
        # DO NOT import json as file

        if _collection in self.db.list_collection_names():
            coll = self.db[_collection]
        else:
            print("WARNING: The collection does not exist")
            exit(1)

        coll.insert_many(json)

        return

def main():
    test = mongodb()
    # #for testing read funcion
    # print(test.get_mongodb_all_json("Transactions"))
    # for item in test.get_mongodb_all_json("Transactions"):
    #     print(item)
    # return

    # # Then test for write
    # file = [{"name":'Carrie Lam', "rating":5},{"name":'Obama', "rating": 9.5}]
    # test.write_mongodb_json('Traders',file)


if __name__ == "__main__":
    main()