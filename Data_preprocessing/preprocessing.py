import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import KNNImputer,SimpleImputer
from imblearn.over_sampling import RandomOverSampler
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from app_logger.logger import app_logger
import pickle
import re

class preprocessing:
    def __init__(self,logfile):
        self.logger = app_logger()
        self.logfile = logfile
    
    def CheckMissingValues(self,data):
        try:
            self.logger.log(self.logfile, "Checking if missing values present in the dataset")
            self.data = data
            self.Null_values_sum = data.isnull().sum()
            self.Present_Null_Values = False
            for i in self.Null_values_sum:
                if i > 0:
                    self.Present_Null_Values = True
                    break
            if self.Present_Null_Values:
                self.logger.log(self.logfile,"There are Missing values in the dataset! \n Check the Null_data.csv in preprocessed data folder to get more information about Null values.")
                Null_data = pd.DataFrame()
                Null_data['columns'] = data.columns
                Null_data['Total_Null_values'] = np.asarray(self.Null_values_sum)
                Null_data.to_csv("preprocessed_data/Null_data.csv")

            return self.Present_Null_Values
        except Exception as e:
            self.logger.log(self.logfile,"Error {e} occured")
            raise e

    def ReplaceInvalidValues(self,data):

        try:
            self.logger.log(self.logfile,"The input dataset contains invalid values in some records like '?'. Replacement of invalid values started!")
            for col in data.columns:
                total = data[col][data[col] == '?'].count()
                if total > 0:
                    data[col] = data[col].replace('?', np.nan)
            self.logger.log(self.logfile,"Replacement of invalid values completed!")
        except Exception as e:
            self.logger.log(self.logfile,"Error {e} occured")
            raise e

    def RemoveNoNeedColumns(self,data,column_list):

        try:
            self.logger.log(self.logfile,"There are some columns that are providing similar information!")
            data = data.drop(column_list,axis = 1)
            self.logger.log(self.logfile,"Columns that are not needed are removed!")
            return data
        except Exception as e:
            self.logger.log(self.logfile,"Error {e} occured")
            raise e
        
    def ImputeMissingValues(self,data):

        try:
            self.logger.log(self.logfile,"Imputing missing values with KNN Imputer>")
            KnnIm=KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
            columns_ = ['TSH','T3', 'TT4','T4U','FTI']

            for i in columns_:
                data[i]=KnnIm.fit_transform(data[[i]])
                self.logger.log(self.logfile,"Missing values replaced!")

            imputer = SimpleImputer(missing_values=np.nan,strategy='most_frequent')
            imputer.fit(data[['sex']])
            data['sex']= imputer.transform(data[['sex']])
            return data
        except Exception as e:
            self.logger.log(self.logfile,"Error {e} occured")
            raise e
    

    def HandliingLabelColumn(self, data, Label_column):
        
        try:
            self.logger.log(self.logfile,"Seting labels for classes!")
            for i in range(len(data[Label_column].unique())):
                pattern = r'[A-Za-z]+'
                if str(data[Label_column][i]).startswith('-'):
                    data[Label_column][i]= "Nagetive"
                else:
                    x = re.findall(pattern,data[Label_column][i])
                    data[Label_column][i] = ''.join(x)
            
            with open('preprocessed_data/labels.txt','w') as file:
                file.write(data[Label_column].unique())
        except Exception as e:
            self.logger.log(self.logfile,"Error {e} occured")
            raise e


    def Categorical_Value_encoding(self,data):

        try:
            self.logger.log(self.logfile,"Encoding categorical data satrted")       
            # Firstly, we encode the sex attribute as it has only two categories.
            data['sex'] = data['sex'].map({'F' : 1,'M': 2})

            # Secondly, Variables with bool value also be encoded
            for var in data.columns:
                if var !='sex' and len(data[var].unique()) == 2:
                    data[var] = data[var].map({'f': 0, 't': 1})

            # We can get dummies for refferal_sources as it has more than two categories and that also are not in order.
            data = pd.get_dummies(data, columns=['referral_source'],prefix='ref')

            # Lastly, we label encode our target attribute "Class"
            encoder = LabelEncoder().fit(data['Class'])
            data['Class'] = encoder.transform(data['Class'])

            with open('pickleFiles/encoder.pickle','wb') as file:
                pickle.dump(encoder,file)
            self.logger.log(self.logfile,"Encoding categorical data Completed")
            return data
        except Exception as e:
            self.logger.log(self.logfile,"Error {e} occured")
            raise e
        

    def SeparateFeaturesAndLabel(self,data,Label_column):
            try:
                self.logger.log(self.logfile,"Separartion of attributes and output started!")
                self.X = data.drop(Label_column,axis=1)
                self.Y = data[Label_column]
                self.logger.log(self.logfile,"Separartion of attributes and output completed!")
                return self.X,self.Y
            except Exception as e:
                self.logger.log(self.logfile,"Error {e} occured")
                raise e
        


    #def ApplyOverSampling(self,X,Y):

       # try:
            #self.logger.log(self.logfile,"Applying Oversampling to the data")
            #rsmaple = RandomOverSampler()
            #x_sample, y_sample = rsmaple.fit_resample(X,Y)

            #return x_sample, y_sample
        #except Exception as e:
           # self.logger.log(self.logfile,"Error {e} occured")
            #raise e


    def scaling(self,X):
        try:
            self.logger.log(self.logfile, "Standard scaling on sampled data has started!")
            self.scaler = StandardScaler()
            self.scaler.fit(X)
            self.scaled_data = self.scaler.transform(X)
            self.new_X = pd.DataFrame(self.scaled_data, columns= X.columns)
            self.logger.log(self.logfile,"Standard scaling has completed!")

            return self.new_X

        except Exception as e:
            self.logger.log(self.logfile,f"Exception {e} ocuured while standardizing the data")
            raise e


