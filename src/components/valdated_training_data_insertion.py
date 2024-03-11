from src.components.RawDataValidation import RawDataValidation as RDV
from src.components.DatabaseOperations import DBOperation as DBO
from src.components.data_Transform import DataTransform as DT
from app_logger.logger import app_logger
import os
from datetime import datetime 


class TrainingDataValidaion:
    def __init__(self,path):
        self.RDV = RDV(path)
        self.DBO = DBO()
        self.DT = DT()
        self.logger = app_logger()

    def TrainDataValdation(self):
        try:
            if not os.path.isdir("Training_Logs/"):
                os.makedirs("Training_Logs/",exist_ok=True)
            file = open("Training_Logs/TrainingDataValidation.txt",'a+')
            self.logger.log(file, 'Validation of Raw Training data started!')
            no_column, column_names = self.RDV.ValueFromSchema()
            self.RDV.ColumnNumbersValidation(no_column)
            self.RDV.validateMissingValuesInColumns()
            self.logger.log(file, 'Validation of Raw Training data Completed!')
            self.logger.log(file, 'Data Transformation started!')
            self.DT.makecolumnstring()
            self.logger.log(file, 'Data Transformation completed!')
            self.logger.log(file, 'Database operation started!')
            self.DBO.CreateTable(column_names,'TrainingData')
            self.logger.log(file, 'Database and table created!')
            self.DBO.InsertDataIntoTable('TrainingData')
            self.logger.log(file, 'Data insertion to the table completed!')
            self.RDV.deleteExistingGoodDataDirectory()

            self.RDV.MoveBadDataToArchive()
            self.logger.log(file, 'Data validation completed!')
            self.logger.log(file, 'Extracting data into csv!')
            self.DBO.DatafromTableToCSV('TrainingData')
            file.close()
        
        except Exception as e:
            file = open("Training_Logs/TrainingDataValidation.txt",'a+')
            self.logger.log(file, 'Exception {e} occured while validating the data')
            file.close()



if __name__ == '__main__':
    path1 = r'C:\Users\49179\Desktop\Machine Learning Projects\Thyroid Detection\Data'
    obj = TrainingDataValidaion(path1)
    obj.TrainDataValdation()