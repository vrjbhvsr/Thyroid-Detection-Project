import pandas as pd
from app_logger.logger import app_logger


class data_loader:
    def __init__(self,logfile):
        self.input_file  = 'Training_file_from_DB\Input.csv'
        self.logfile = logfile
        self.logger = app_logger()

    def get_data(self):
        try:
            self.logger.log(self.logfile,"Data loading has started!")
            self.data = pd.read_csv(self.input_file)
            self.logger.log(self.logfile,"Data loaded succesfully!")
            return self.data
        

        except Exception as e:
            self.logger.log(self.logfile,"Error {e} occured while loading the data!\n Data loadding Unsuccesfull.")
            raise e