from datetime import datetime
from app_logger.logger import app_logger
import json
import os
import shutil
import pandas as pd


class RawDataValidation:
    def __init__(self,raw_data_direcotry):
        self.logger = app_logger()
        self.raw_data_directory = raw_data_direcotry
        self.schema = 'schema_training.json'


    def ValueFromSchema(self):
        try:
            
            with open(self.schema,'r') as f:
                name = json.load(f)
                f.close()
            no_columns = name["NumberofColumns"]
            column_names = name["ColName"]
            with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                self.logger.log(log,f'Total number of columns are {no_columns}')

            return no_columns,column_names
        except Exception as e:
            with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                self.logger.log(log,e)


    def GoodBadDataDirectory(self):
        try:
            path = os.path.join("Training_data_validated/","GoodRawData/")
            if not os.path.isdir(path):
                os.makedirs(path)
                with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                    self.logger.log(log,f'GoodRawData directory created')
            path = os.path.join("Training_data_validated/","BadRawData/")
            if not os.path.isdir(path):
                os.makedirs(path)
                with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                    self.logger.log(log,f'BadRawData directory created')
        except Exception as e:
            with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                self.logger.log(log,f'Exception {e} occured while creating a directory')


    def deleteExistingGoodDataDirectory(self):
        try:
            path = "Training_data_validated/"
            if os.path.isdir(path + 'GoodRawData/'):
                shutil.rmtree(path + 'GoodRawData/')
                with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                    self.logger.log(log,f'GoodRawData directory deleted!')
        except Exception as e:
            with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                self.logger.log(log,f'Exception {e} occured while creating a directory')


    def deleteExistingBadDataDirectory(self):
        try:
            path = "Training_data_validated/"
            if os.path.isdir(path + 'BadRawData/'):
                shutil.rmtree(path + 'BadRawData/')
                with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                    self.logger.log(log,f'BadRawData directory deleted!')
        except Exception as e:
            with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                self.logger.log(log,f'Exception {e} occured while creating a directory')

    def MoveBadDataToArchive(self):
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = "Training_data_validated/BadRawData/"
            if os.path.isdir(source):
                path = 'ArchivedBadDataDirectory'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = "ArchivedBadDataDirectory/BadData_" + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    shutil.move(source + f,dest)
                    with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                        self.logger.log(log, 'Bad data from BadDataDirectory is moved to Archive folder!')
                path = "Training_data_validated/BadRawData/"
                if os.path.isdir(path):
                    shutil.rmtree(path)
        except Exception as e:
            with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                self.logger.log(log,f'Exception {e} occured while Moving a directory')


    def ColumnNumbersValidation(self,NumberOfColumns):
        try:
            with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                        self.logger.log(log, 'Validation of number of columns started!')

            self.deleteExistingGoodDataDirectory()
            self.deleteExistingBadDataDirectory()
            self.GoodBadDataDirectory()

            files = [f for f in os.listdir(self.raw_data_directory)]
            Gpath = "Training_data_validated/GoodRawData/"
            Bpath = "Training_data_validated/BadRawData/"
            
            try:
                for f in files:
                    csv = pd.read_csv(self.raw_data_directory+ '/' + f)
                    if csv.shape[1] == NumberOfColumns:
                        shutil.copy(self.raw_data_directory+ '/' + f, Gpath)
                        with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                            self.logger.log(log, 'Column numbers are Validated and file copied to the GoodRawData directory!')
                    else:
                        shutil.move(self.raw_data_directory+ '/' + f, Bpath)
                        with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                            self.logger.log(log, 'Column numbers not as per requirement and file moved to the BadRawData directory!')
            except Exception as e:
                with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                    self.logger.log(log,f'Exception {e} occured')
        except Exception as e:
                with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                    self.logger.log(log,f'Exception {e} occured while Valiating Column number')
    
    def validateMissingValuesInColumns(self):
        try:
            with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                        self.logger.log(log, 'Validation of missing values in columns started!')

            path = "Training_data_validated/GoodRawData/"
            for file in os.listdir(path):
                csv = pd.read_csv(path + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns])-csv[columns].count()) == len(csv[columns]):
                        count += 1
                        shutil.move( path +file, "Training_data_validated/BadRawData")
                        break
                if count == 0:
                    csv.to_csv(path + file,index = None, header =True)
        except Exception as e:
                with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                    self.logger.log(log,f'Exception {e} occured while Valiating Column number')
