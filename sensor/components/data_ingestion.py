from sensor import utils
from sensor.logger import logging
from sensor.exception import SensorException
from sensor.entity import artifact_entity
from sensor.entity import config_entity
import os, sys
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

class DataIngestion:

    def __init__(self,data_ingestion_config:config_entity.DataIngestionConfig):
        try:
            logging.info(f"{'>>' * 20} Data Ingestion {'<<'*20}")
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise SensorException(e, sys)
    
    def initiate_data_ingestion(self,)->artifact_entity.DataIngestionArtifact:
        try:

            logging.info("Exporting collection data as pandas Dataframe")
            #Exporting collection data as pandas Dataframe
            df:pd.DataFrame = utils.get_collection_as_dataframe(
                database_name=self.data_ingestion_config.database_name, 
                collection_name=self.data_ingestion_config.collection_name)

            logging.info("replacing na with np.NAN")
            #replacing na with np.NAN
            df.replace(to_replace='na', value=np.NAN, inplace=True)

            logging.info("Save Data in Feature Store")
            #Create feature Store folder is not avaiable
            feature_store_dir = os.path.dirname(self.data_ingestion_config.feature_store_file_path)
            os.makedirs(feature_store_dir, exist_ok=True)
            #Save Data in Feature Store
            df.to_csv(path_or_buf=self.data_ingestion_config.feature_store_file_path,index=False,header=True)

            logging.info("Create dataset folder is not avaiable")
            #Create dataset folder is not avaiable
            dataset_dir = os.path.dirname(self.data_ingestion_config.train_file_path)
            os.makedirs(name=dataset_dir,exist_ok=True)

            logging.info("Split dataset into trian and test")
            #Split dataset into trian and test
            trian_df, test_df = train_test_split(df,test_size=self.data_ingestion_config.test_size)

            logging.info("Save trian and test data into Dataset folder")
            #Save trian and test data into Dataset folder
            df.to_csv(path_or_buf=self.data_ingestion_config.train_file_path,index=False,header=True)
            df.to_csv(path_or_buf=self.data_ingestion_config.test_file_path,index=False,header=True)

            #Prepaer artifacts

            data_ingestion_artifact = artifact_entity.DataIngestionArtifact(
                feature_store_file_path=self.data_ingestion_config.feature_store_file_path,
                train_file_path=self.data_ingestion_config.train_file_path,
                test_file_path=self.data_ingestion_config.test_file_path
                )

            logging.info(f"Data Ingestion Artifact : {data_ingestion_artifact}")

            return data_ingestion_artifact

        except Exception as e:
            raise SensorException(e, sys)

