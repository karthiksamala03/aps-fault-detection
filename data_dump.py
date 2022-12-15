import pymongo
import pandas as pd
import json


# Provide the mongodb localhost url to connect python to mongodb.
#client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

from sensor.config import mongo_client
client = mongo_client

DATA_FILE_PATH="/config/workspace/aps_failure_training_set1.csv"
DATABASE_NAME="aps"
COLLECTION_NAME="sensor"

if __name__=="__main__":
    df = pd.read_csv(DATA_FILE_PATH)
    print(f"Rows and columns : {df.shape}")

    # Convert dataframe to json formmat so that we can dump this records in mongo db
    df.reset_index(drop=True, inplace=True)

    # json.load --> convert any objects 
    # convert to list format
    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    # inserting records into mongo
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)
