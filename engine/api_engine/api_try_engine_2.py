import certifi
from pymongo import MongoClient

class Api_Mongodb:
    def __init__(self,database):
        try:
            self.conn = MongoClient('mongodb+srv://Garylam:Lamindexinvest123!@mathtrade.yvcna.mongodb.net/?retryWrites=true&w=majority',tlsCAFile=certifi.where())
            self.db = self.conn[database]
            print(f"Successful connection to database: {self.db.name}")
        except:
            print("WARNING: Could not connect to MongoDB")

    def all_algo_4a(self):
        mongo = Api_Mongodb("rainydrop")
        col = mongo.db.Strategies

    def convert_df_to_json(self, df):
        json_file = df.to_json(orient='records')
        return json_file

def main():
    pass
    # all_algo_1a()
    # all_algo_1b()

if __name__ == "__main__":
        main()