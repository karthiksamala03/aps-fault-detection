import os,sys
from sensor.logger import logging
from sensor.exception import SensorException

if __name__=="__main__":
     logging.info("This is my first logging")
     try:
          logging.info("performing div operation")
          result = 3/0
          print(result)
          logging.info("operation successfully completed")
     except Exception as e:
          logging.info(f"Error message {e}")