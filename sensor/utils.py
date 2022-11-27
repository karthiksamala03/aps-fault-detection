import os, sys
from sensor.logger import logging
from sensor.exception import SensorException
from sensor.config import mongo_client
import pandas as pd

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
        logging.info(f"{df.columns")
        if "_id" in df.columns:
            df.drop("_id", axis=1)
        logging.info(f"Rows and Columns in DataFrame {df.shape}")

        return df 
        
    except Exception as e:
        raise SensorException(e, sys)



