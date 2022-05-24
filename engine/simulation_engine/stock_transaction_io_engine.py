import csv
import os
import pathlib

class stock_transaction_io_engine:
    output_filepath = ""
    fieldname = None

    def __init__(self):
        self.output_filepath = str(pathlib.Path(__file__).parent.parent.parent.resolve()) + "/real_time_data/transaction_record.csv"
        self.fieldname = ["state","timestamp","orderId", "ticker","action","lmtPrice","totalQuantity","avgPrice","error message","exchange","commission"]

    def put_single_transaction_record(self,action_msg):
        if "transaction_record.csv" not in os.listdir(str(pathlib.Path(self.output_filepath).parent.resolve())):
            with open(self.output_filepath,'a+',newline='') as f:
                writer = csv.DictWriter(f,self.fieldname)
                writer.writeheader()
                writer.writerow(action_msg)
        else:
            with open(self.output_filepath,'a+',newline='') as f:
                writer = csv.DictWriter(f,self.fieldname)
                writer.writerow(action_msg)

        print("Trade record written successfully.")

    # action_msg_list show be a list of dictionary 
    def put_transaction_record_list(self,action_msg_list):
        if "transaction_record.csv" not in os.listdir(str(pathlib.Path(self.output_filepath).parent.resolve())):
            with open(self.output_filepath,'a+',newline='') as f:
                writer = csv.DictWriter(f,self.fieldname)
                writer.writeheader()
                writer.writerows(action_msg_list)
        else:
            with open(self.output_filepath,'a+',newline='') as f:
                writer = csv.DictWriter(f,self.fieldname)
                writer.writerows(action_msg_list)

        print("Trade record written successfully.")

def main():
    engine = stock_transaction_io_engine()
    engine.put_transaction_record_list([{"orderId":1},{"action":"buy"}])

if __name__ == "__main__":
    main()