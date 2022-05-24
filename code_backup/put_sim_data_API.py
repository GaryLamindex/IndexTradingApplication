import os
import sys

# grab the dynamo_db_engine class
script_dir = os.path.dirname(__file__)
db_engine_dir = os.path.join(script_dir, '..', 'aws_engine')
sys.path.append(db_engine_dir)

import dynamo_db_engine as db_engine

class put_sim_data_API:
    db_engine = None

    def __init__(self):
        self.db_engine = db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
    
    # expected data format: array of dictionary
    # example: [{"spec":"spec1","timestamp":1234567},{"spec":"spec1","timestamp":23456789}]
    def upload_sim_data(self,table_name,items):
        table = self.db_engine.dynamodb.Table(table_name)
        for item_dict in items:
            try:
                table.put_item(Item=item_dict)
            except Exception as e:
                print("Item -", item_dict)
                print("Error message -", e.response['Error']['Message'])

def main():
    my_API = put_sim_data_API()
    my_API.upload_sim_data('ticket_test',[{"spec":"spec1","timestamp":1234567},{"spec":"spec1","timestamp":23456789}])

if __name__ == "__main__":
    main()