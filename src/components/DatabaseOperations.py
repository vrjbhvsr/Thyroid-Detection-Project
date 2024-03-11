import sqlite3
from app_logger.logger import app_logger
import csv
import os
import shutil

class DBOperation:
    def __init__(self):
        self.path = 'Training_Database/'
        self.GoodDatapath = "Training_data_validated/GoodRawData/"
        self.BadDatapath = "Training_data_validated/BadRawData/"
        self.logger = app_logger()

    def MakeConnectionToDB(self,Databasename):
        try:
                
            if not os.path.isdir(self.path):
                os.makedirs(self.path)
            connection = sqlite3.connect(self.path+Databasename + '.db')
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                        self.logger.log(log, f'Connection with {Databasename} database established!')
            return connection
        except Exception as e:
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                        self.logger.log(log, f'Error {e} occured while Connecting with {Databasename} database!')
        
    def CreateTable(self,column_names,Databasename):
        try:
            connection = self.MakeConnectionToDB(Databasename)
            cur = connection.cursor()
            cur.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' and name = 'GoodRawData'")
            if cur.fetchone()[0] == 1:
                connection.close()
                with open("Training_Logs/DBOperation.txt",'a+') as log:
                            self.logger.log(log, f'Table Already Exists!')
            else:
                for keys in column_names.keys():
                    dtypes = column_names[keys]
                    try:
                        cur.execute("ALTER TABLE GoodRawData ADD COLUMN '{Cname}'{value}".format(Cname = keys, value = dtypes))
                    except:
                        cur.execute("CREATE TABLE GoodRawData({Cname}{value})".format(Cname = keys, value = dtypes))
                with open("Training_Logs/DBOperation.txt",'a+') as log:
                        self.logger.log(log, f'Table created succesfully!')
                connection.close()
        except Exception as e:
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                        self.logger.log(log, f'Error {e} occured while creating Table GoodRawData!')
    
    
    def InsertDataIntoTable(self,Databasename):
        try:
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                        self.logger.log(log, f'Data insertion from GoodRawData file to table started!') 
            connection = self.MakeConnectionToDB(Databasename)
            cursor = connection.cursor()
            

            files = [f for f in os.listdir(self.GoodDatapath)]

            for file in files:
                with open(self.GoodDatapath+file ,'r') as f:
                    next(f)
                    read = csv.reader(f,delimiter='\n')
                    for l in read:
                        row_values = l[0].split(",")
                        cursor.execute('INSERT INTO GoodRawData VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            row_values)
                        connection.commit()
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                self.logger.log(log, f'Data Succesfully inserted into Table!')
            connection.close()

        except Exception as e:
            connection.rollback()
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                    self.logger.log(log, f'Error {e} occured while Insertng data into table!')
            shutil.move(self.GoodDatapath + f ,self.BadDatapath)
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                    self.logger.log(log, "File moved to BadDataRaw directory")

    def DatafromTableToCSV(self,Databasename):
        try:
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                        self.logger.log(log, f'selection of data from table to csv started!')
            self.db_path = 'Training_file_from_DB/'
            self.filename = "Input.csv"

            connection = self.MakeConnectionToDB(Databasename)
            cursor = connection.cursor()
            query = "SELECT * FROM GoodRawData"
            cursor.execute(query)
            result = cursor.fetchall()

            header = [i[0] for i in cursor.description]

            if not os.path.isdir(self.db_path):
                os.makedirs(self.db_path)
                with open("Training_Logs/DBOperation.txt",'a+') as log:
                        self.logger.log(log, f'{self.db_path} directory created!')

            csv_file = csv.writer(open(self.db_path+self.filename, 'w', newline=""),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')
            csv_file.writerow(header)
            csv_file.writerows(result)
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                        self.logger.log(log, f'Rows and columns are written from table to Input.csv!')
        except Exception as e:
            with open("Training_Logs/DBOperation.txt",'a+') as log:
                        self.logger.log(log, f'Error {e} occured while selecting data from table to csv!')