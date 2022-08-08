from engine.mongoDB_engine.write_mongo_db_modified import Write_Mongodb


class RealtimeUpdateDbInfo:
    def run(self):
        while True:
            engine = Write_Mongodb()
            engine.run_all()

