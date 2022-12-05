import os, sys
from sensor.logger import logging
from sensor.exception import SensorException
from sensor.config import mongo_client
import pandas as pd
import yaml

def get_collection_as_dataframe(database_name:str, collection_name:str)->pd.DataFrame:
    """
    Description : This function return collection as Dataframe
    ==========================================================
    Params:
    database_name : database name
    collection_name : collection name 
    ==========================================================
    return: Pandas DataFrame
    """
    try:
        logging.info(f"Reading Database {database_name} and collection {collection_name}")
        df = pd.DataFrame(list(mongo_client[database_name][collection_name].find()))
        logging.info(f"{df.columns}")
        if "_id" in df.columns:
            df.drop("_id", axis=1)
        logging.info(f"Rows and Columns in DataFrame {df.shape}")

        return df 

    except Exception as e:
        raise SensorException(e, sys)

def convert_column_float(df:pd.DataFrame, exclude_columns:list)->pd.DataFrame:
    try:
        for col in df.columns:
            if col not in exclude_columns:
                df[col] = df[col].astype('float')

        return df
    except Exception as e:
        raise SensorException(e, sys)

def write_yaml_file(file_path:str, data:dict):
    try:
        file_dir = os.path.dirname(p=file_path)
        os.makedirs(file_dir, exist_ok=True)
        with open(file_path,"w") as file_writer:
            yaml.dump(data,file_path)

    except Exception as e:
        raise SensorException(e, sys)

