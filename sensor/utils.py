import os, sys
from sensor.logger import logging
from sensor.exception import SensorException
from sensor.config import mongo_client
import pandas as pd
import numpy as np
import yaml
import dill

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
            df = df.drop("_id", axis=1)
            logging.info(f"_id column droped successfully")
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
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok=True)
        with open(file_path,"w") as file_writer:
            yaml.dump(data,file_writer)

    except Exception as e:
        raise SensorException(e, sys)

def save_numpy_array_data(file_path:str, array:np.array):
    """
    save numpy array data to file
    file_path : str location of file to save
    array : np.array data to save
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path,"wb") as file_obj:
            np.save(file_obj, array)

    except Exception as e:
        raise SensorException(e, sys)

def load_numpy_array_data(file_path:str)->np.array:
    """
    load numpy array data from file
    file path : str location of file to load
    return : np.array data loaded
    """
    try:
        if not os.path.exists(path=file_path):
            raise Exception(f"The file : {file_path} not exist")
        with open(file_path,"rb") as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise SensorException(e, sys)

def save_object(file_path: str, obj: object) -> None:
    try:
        logging.info("Entered the save_object method of utils")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)
        logging.info("Exited the save_object method of utils")
    except Exception as e:
        raise SensorException(e, sys) from e


def load_object(file_path:str)->object:
    try:
        if not os.path.exists(path=file_path):
            raise Exception(f"The file : {file_path} not exist")
        with open(file_path,"rb") as file_obj:
            return dill_load(file_obj)
    except Exception as e:
        raise SensorException(e, sys)