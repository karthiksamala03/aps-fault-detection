import os
from sensor.logger import logging
from sensor.exception import SensorException
from typing import Optional
from sensor.entity.config_entity import MODEL_FILE_NAME, TRANSFORMATOR_OBJECT_FILE_NAME, TARGET_ENCODER_OBJECT_FILE_NAME

class ModelResolver:
    
    def __init__(self,
                model_registy="saved_models",
                transformer_dir_name="transformer",
                target_encoder_dir_name="target_encoder",
                model_dir_name="model"):
        try:
            self.model_registy=model_registy
            os.makedirs(name=self.model_registy, exist_ok=True)
            self.transformer_dir_name=transformer_dir_name
            self.target_encoder_dir_name=target_encoder_dir_name
            self.model_dir_name=model_dir_name
        except Exception as e:
            raise SensorException(e, sys)

    def get_latest_dir_path(self)->Optional[str]:
        try:
            dir_names = os.listdir(self.model_registy)
            dir_names = list(map(int,dir_names))
            if len(dir_names) == 0:
                return None
            latest_dir_name = max(dir_names)
            return os.path.join(self.model_registy,f"{latest_dir_name}")

        except Exception as e:
            raise SensorException(e, sys)

    def get_latest_model_path(self)->Optional[str]:
        try:
            latest_dir = self.get_latest_dir_path()
            if latest_dir == None:
                raise Exception(f"Model is not avaiable")
            return os.path.join(latest_dir,self.model_dir_name,MODEL_FILE_NAME)
        except Exception as e:
            raise SensorException(e, sys)

    def get_latest_transformer_path(self)->Optional[str]:
        try:
            latest_dir = self.get_latest_dir_path()
            if latest_dir == None:
                raise Exception(f"Transformer is not avaiable")
            return os.path.join(latest_dir,self.transformer_dir_name,TRANSFORMATOR_OBJECT_FILE_NAME)
        except Exception as e:
            raise SensorException(e, sys)

    def get_latest_target_encoder_path(self)->Optional[str]:
        try:
            latest_dir = self.get_latest_dir_path()
            if latest_dir == None:
                raise Exception(f"Target Encoder is not avaiable")
            return os.path.join(latest_dir,self.target_encoder_dir_name,TARGET_ENCODER_OBJECT_FILE_NAME)
        except Exception as e:
            raise SensorException(e, sys)




    def get_latest_save_dir_path(self)->Optional[str]:
        try:
            latest_dir = self.get_latest_dir_path()
            if len(dir_names) == 0:
                return None
            latest_dir_num = int(os.path.basename(latest_dir))
            return os.path.join(self.model_registy,f"{latest_dir_num+1}")

        except Exception as e:
            raise SensorException(e, sys)

    def get_latest_model_path(self)->Optional[str]:
        try:
            latest_dir = self.get_latest_save_dir_path()
            return os.path.join(latest_dir,self.model_dir_name,MODEL_FILE_NAME)
        except Exception as e:
            raise SensorException(e, sys)

    def get_latest_transformer_path(self)->Optional[str]:
        try:
            latest_dir = self.get_latest_save_dir_path()
            return os.path.join(latest_dir,self.transformer_dir_name,TRANSFORMATOR_OBJECT_FILE_NAME)
        except Exception as e:
            raise SensorException(e, sys)

    def get_latest_target_encoder_path(self)->Optional[str]:
        try:
            latest_dir = self.get_latest_save_dir_path()
            return os.path.join(latest_dir,self.target_encoder_dir_name,TARGET_ENCODER_OBJECT_FILE_NAME)
        except Exception as e:
            raise SensorException(e, sys)

