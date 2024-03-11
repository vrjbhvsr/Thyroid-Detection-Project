import pandas as pd
from datetime import datetime
import os
from app_logger.logger import app_logger


class DataTransform:
    def __init__(self):
        self.GoodDatapath = "Training_data_validated/GoodRawData/"
        self.logger = app_logger()

    def makecolumnstring(self):
        try:
            files = [file for file in os.listdir(self.GoodDatapath)]
            for f in files:
                csv = pd.read_csv(self.GoodDatapath+f)
                columns = ['sex', 'on_thyroxine', 'query_on_thyroxine', 'on_antithyroid_medication', 'sick', 'pregnant',
                            'thyroid_surgery', 'I131_treatment', 'query_hypothyroid', 'query_hyperthyroid', 'lithium',
                            'goitre', 'tumor', 'hypopituitary', 'psych', 'TSH_measured', 'T3_measured', 'TT4_measured',
                            'T4U_measured', 'FTI_measured', 'TBG_measured', 'TBG', 'referral_source', 'Class']

                for column in csv.columns:
                    if column in columns:
                        csv[column] = csv[column].apply(lambda x: "'" + str(x) + "'")
                    if column not in columns:
                        csv[column] = csv[column].replace('?',"'?'")
            with open("Training_Logs/datatransform.txt",'a+') as log:
                            self.logger.log(log, 'Column names azre converted into string!')
            csv.to_csv(self.GoodDatapath + f, index=None,header =True)
        except Exception as e:
             with open("Training_Logs/RawDataValidation.txt",'a+') as log:
                        self.logger.log(log, 'Exception {e} occured!')  