import pandas as pd
import os

FILE_PATH = "C:/Users/85266/OneDrive/Documents"

def combine_csv(filepath,csv_list,output_name):
    df_list = []
    for csv in csv_list:
        df = pd.read_csv(f"{filepath}/{csv}")
        df_list.append(df)
        
    full_df = pd.concat(df_list)

    # remove the output file if exist
    file_exist = f"{output_name}.csv" in os.listdir(filepath)
    if file_exist:
        try:
            os.remove(f"{FILE_PATH}/{output_name}.csv")
        except Exception as e:
            print(f"Some errors occur, error message: {e}")

    # write the entire df to the output file
    with open(f"{FILE_PATH}/{output_name}.csv","a+",newline='') as f:
        full_df.to_csv(f,mode='a',index=False,header=True) # write the current data with header
    print("Successfully combined csv files")

def main():
    combine_csv(FILE_PATH,["GOVT_1.csv","GOVT_2.csv"],"GOVT")

if __name__ == "__main__":
    main()