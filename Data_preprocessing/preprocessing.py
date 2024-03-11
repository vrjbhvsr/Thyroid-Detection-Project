import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import KNNImputer
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
            data['sex'] = data['sex'].map({'M' : 0,'F': 1})

            categorical_data = data.drop(['age','T3','TT4','T4U','FTI','sex'],axis=1)

            # Secondly, Variables with bool value also be encoded
            for var in categorical_data.columns:
                if len(data[var].unique()) == 2:
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
        

    def ImputeMissingValues(self,data):

        try:
            self.logger.log(self.logfile,"Imputing missing values with KNN Imputer>")
            KnnIm=KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
            Imp=KnnIm.fit_transform(data)
            data=pd.DataFrame(data=Imp, columns=data.columns)
            self.logger.log(self.logfile,"Missing values replaced!")
            return data
        except Exception as e:
            self.logger.log(self.logfile,"Error {e} occured")
            raise e


    def ApplyOverSampling(self,X,Y):

        try:
            self.logger.log(self.logfile,"Applying Oversampling to the data")
            rsmaple = RandomOverSampler()
            x_sample, y_sample = rsmaple.fit_resample(X,Y)

            return x_sample, y_sample
        except Exception as e:
            self.logger.log(self.logfile,"Error {e} occured")
            raise e


    def scaling(self,x_sample,y_sample):
        try:
            self.logger.log(self.logfile, "Standard scaling on sampled data has started!")
            self.scaler = StandardScaler()
            self.scaler.fit(x_sample)
            self.scaled_data = self.scaler.transform(x_sample)
            self.logger.log(self.logfile,"Standard scaling has completed!")

            return self.scaled_data

        except Exception as e:
            self.logger.log(self.logfile,f"Exception {e} ocuured while standardizing the data")
            raise e
        

    def ExtractingFeatureWithPCA(self,x_sample,scaled_data):
        try:
            self.logger.log(self.logfile,"Feature Extraction using pca started!")
            pca = PCA(n_components=12)
            self.x_pca = pca.fit_transform(scaled_data)
            self.X_pca = pd.DataFrame(data = self.x_pca,columns=['component_1','component_2','component_3','component_4','component_5','component_6','component_7','component_8','component_9','component_10','component_11','component_12'])
            self.logger.log(self.logfile,"Feature has Extracted using PCA")
            self.X_pca.to_csv('preprocessed_data/data_pca.csv')

            return self.X_pca
        
        except Exception as e:
            self.logger.log(self.logfile,"Feature extraction completed.")
            raise e


