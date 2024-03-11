import pandas as pd
from app_logger.logger import app_logger
from datetime import datetime
import shutil
import os
import json

class one_CSV:
    def __init__(self, raw_data_directory):
        self.logger = app_logger()
        self.raw_data_directory = raw_data_directory
        self.schema = 'schema_training.json'


    def create_data_directory(self):
        try:
            os.makedirs("Data/", exist_ok=True)
            os.makedirs("Preparatory_logs/", exist_ok=True)
        except OSError as e:
            with open("Preparatory_logs/directorylog.txt", "a+") as file:
                self.logger.log(file, f'OSError: Error in directory creation {e}')
            raise OSError
        
    def train_dataset(self,data_file):
        try:
            with open('Preparatory_logs/dataset_preparation.txt',"a+") as file:
                self.logger.log(file,"Column name extraction from schema has started!")
            with open(self.schema,'r') as f:
                name = json.load(f)
                f.close()
            column_names = name['ColName']
            number_of_columns = name['NumberofColumns']
            with open('Preparatory_logs/dataset_preparation.txt',"a+") as file:
                self.logger.log(file,"Column name extraction from schema has completed! Now, Creating a final dataset with column names.")
            data = pd.read_csv(data_file,header=None)
            if data is not None and column_names is not None:
                data.columns = column_names.keys()
                data_file_path = os.path.join(self.raw_data_directory,"Final_train_data.csv")
                data.to_csv(data_file_path,index=False)
                shutil.move(data_file_path,'Data/')
                with open('Preparatory_logs/dataset_preparation.txt',"a+") as file:
                    self.logger.log(file,"Final dataset has prepared and moved to the Data directory for further processing.")
        except Exception as e:
            with open('Preparatory_logs/dataset_preparation.txt',"a+") as file:
                    self.logger.log(file,"Final dataset has prepared and moved to the Data directory for further processing.")


if __name__ == "__main__":
    raw_data_dir = r'C:\Users\49179\Desktop\Machine Learning Projects\Thyroid Detection\raw_data'
    obj = one_CSV(raw_data_dir)
    obj.create_data_directory()
    record_file = os.path.join(raw_data_dir, "thyroid0387.data")
    obj.train_dataset(record_file)
