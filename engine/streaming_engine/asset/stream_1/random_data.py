from decimal import Decimal
from time import sleep
import random
import datetime as dt
import csv
import os

os.path.dirname(os.path.realpath(__file__))
def generate_random_data():
    current_dir=os.path.dirname(os.path.realpath(__file__))
    directory=os.path.join(current_dir,'real_time.csv')
    while True:
        timestamp = int(dt.datetime.now().timestamp())
        Net_liquidation = str((1 + random.random()) * 1000000)
        Market_price=str((3+random.random())*100)
        volume=str((1+random.random())*1000000)
        YTD=random.random()*100
        one_year =random.random()*100
        five_year=random.random()*100
        list=[timestamp,Net_liquidation,Market_price,volume,YTD,one_year,five_year]
        with open(directory,'a',newline='') as f_object:
            writer_object = csv.writer(f_object)
            # Result - a writer object
            # Pass the data in the list as an argument into the writerow() function
            writer_object.writerow(list)  
            # Close the file object
            f_object.close()
            print('Successfully updated!')
            sleep(20)

def main():
    generate_random_data()

if __name__ == "__main__":
    main()