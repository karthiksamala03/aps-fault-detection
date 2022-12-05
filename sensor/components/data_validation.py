from sensor.entity import artifact_entity, config_entity
from sensor.exception import SensorException
from sensor.logger import logging
from sensor import utils
import os, sys
from scipy.stats import ks_2samp
from sensor.config import TARGET_COLUMN
import pandas as pd
import numpy as np
from typing import Optional
from sensor.config import TARGET_COLUMN

class DataValidation:

    def __init__(self, data_validation_config:config_entity.DataValidationConfig, 
                data_ingestion_artifact:artifact_entity.DataIngestionArtifact):
        try:
            logging.info(f" {'>>' * 20} Data Validation {'<<' *20} ")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.validation_error = dict()
        except Exception as e:
            raise SensorException(error_message = e, error_detail = sys)

    def drop_missing_values_columns(self, df:pd.DataFrame, report_key_name:str)->Optional[pd.DataFrame]:
        """
        This function will drop columns which contain missing values more than specified threshold

        df : Accepts Pandas DataFrame
        report_key_name : report name 
        threshold : Percentage criteria to drop a column
        =====================================================================================
        returns Pandas DataFrame if atleast single column is available after droping missing columns else None
        """
        try:
            threshold = self.data_validation_config.missing_threshold
            null_report = np.na().sum()/df.shape[0]

            logging.info(f"selecting column names which contains null values above {threshold}")
            droping_columns_names = null_report[null_report>0.2].index

            logging.info(f"Columns to drop : {list(droping_columns_names)}")
            self.validation_error[report_key_name] = list(droping_columns_names)
            df.drop(list(droping_columns_names), axis=1, inplace=True)

            # return None if no columns left
            if len(df) == 0:
                return None
            return df

        except Exception as e:
            raise SensorException(e, sys)


    def is_required_columns_exists(self, base_df:pd.DataFrame, current_df:pd.DataFrame, report_key_name:str)->bool:
        try:
            
            base_columns = base_df.columns
            current_columns = current_df.columns

            missing_columns = []
            for base_column in base_columns:
                if base_column not in current_columns:
                    logging.info(f"column : {base_column} is not available") 
                    missing_columns.append(base_column)

            if len(missing_columns) > 0:
                self.validation_error[report_key_name] = missing_columns
                return False
            return True

        except Exception as e:
            raise SensorException(e, sys)

    def data_drift(self, base_df:pd.DataFrame, current_df:pd.DataFrame, report_key_name:str):
        try:
            drift_report = dict()

            base_columns = base_df.columns
            current_columns = current_df.columns

            for base_column in base_columns:
                base_data, current_data = base_df[base_column], current_df[base_column]
                #Null Hypothesis is that both data drawn from same distribution

                logging.info(f"{base_column} : base_datatype = {base_data.dtype}, current_datatype = {current_data.dtype}")
                distribution = ks_2samp(base_data, current_data)

                # Same Distribution if pvalue > 0.05 else Diff distribution
                if distibution.pvalue>0.05:
                    drift_report[base_column] = {
                        'p-value' : float(distibution.pvalue),
                        'same distribution' : True
                    }
                else:
                    drift_report[base_column] = {
                        'p-value' : float(distibution.pvalue),
                        'same distribution' : False
                    }

                self.validation_error[report_key_name] = drift_report

        except Exception as e:
            raise SensorException(e, sys)

    def initiate_data_validation(self)->artifact_entity.DataValidationArtifact:
        try:
            logging.info(f"reafing base dataframe")
            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            logging.info("replacin NAN values in base df")
            base_df.replace({'na':np.NAN}, inplace=True)

            logging.info("Drop null value columns from base df")
            base_df = self.drop_missing_values_columns(df=base_df, report_key_name='missing_values_within_base_dataset')

            logging.info(f"reading train dataframe")
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            logging.info(f"reading test dataframe")
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)

            # logging.info("replacing NAN values in train df")
            # train_df.replace({'na':np.NAN}, inplace=True)
            # logging.info("replacing NAN values in test df")
            # test_df.replace({'na':np.NAN}, inplace=True)

            logging.info("Drop null value columns from train df")
            train_df = self.drop_missing_values_columns(df=train_df, report_key_name='missing_values_within_trian_dataset')
            logging.info("Drop null value columns from test df")
            test_df = self.drop_missing_values_columns(df=test_df, report_key_name='missing_values_within_test_dataset')

            exclude_columns = [TARGET_COLUMN]
            logging.info("")
            base_df = utils.convert_column_float(df=base_df, exclude_columns=exclude_columns)
            train_df = utils.convert_column_float(df=train_df, exclude_columns=exclude_columns)
            test_df = utils.convert_column_float(df=test_df, exclude_columns=exclude_columns)

            logging.info("Is all required columns available in train df")
            trian_df_column_status = self.is_required_columns_exists(base_df=base_df, current_df=train_df, report_key_name="missing_values_within_train_dataset")            
            logging.info("Is all required columns available in test df")
            test_df_column_status = self.is_required_columns_exists(base_df=base_df, current_df=test_df, report_key_name="missing_values_within_test_dataset")

            if trian_df_column_status:
                logging.info("As all columns are available in train df hence detecting data drift")
                self.data_drift(base_df=base_df, current_df=train_df, report_key_name="data_drift_with_train_dataset")

            if test_df_column_status:
                logging.info("As all columns are available in test df hence detecting data drift")
                self.data_drift(base_df=base_df, current_df=test_df, report_key_name="data_drift_with_test_dataset")

            
            #write the report
            logging.info("write report to yaml file")
            utils.write_yaml_file(file_path=self.data_validation_config.report_file_path, data=self.validation_error)

            data_validation_artifact = artifact_entity.DataValidationArtifact(report_file_path=self.data_validation_config.report_file_path)

            logging.info(f"Data Validation Artifact:  {data_validation_artifact}")

            return data_validation_artifact

        except Exception as e:
            raise SensorException(e, sys)

 

    
