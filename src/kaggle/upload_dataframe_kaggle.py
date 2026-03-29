from kagglehub import dataset_upload
from kagglehub.auth import set_kaggle_api_token
from pyspark.sql import SparkSession
from tempfile import TemporaryDirectory
from datetime import datetime
import os

from src.utils.load_env import get_env
from src.utils.logger import get_logger,setup_logger
from src.kaggle.utils import read_sql_tables, write_csv_tables

## env credentials
config = get_env()                                                                       
                                                                       
## logging                                                                       
setup_logger(root_logger_name=config.APPLICATION_LOG_NAME,                                                                       
             log_path=config.LOG_PATH,                                                                       
             log_file_name=config.LOG_FILE_NAME,                                                                       
             log_level=config.LOG_LEVEL,                                                                       
             azure_connection_string= config.LOG_CONNECTION_STRING)                                                                       
logger = get_logger(config.APPLICATION_LOG_NAME,__name__)                                                                       
                                                                       
set_kaggle_api_token(api_token=config.KAGGLE_API_TOKEN)

spark = (SparkSession.builder
        .appName("Kaggle_dataset_upload")
        .config("spark.jars.packages",
                ",".join(["com.mysql:mysql-connector-j:8.0.33",
                        "com.azure:azure-storage-blob:12.24.0"
                        ])
                )
        .getOrCreate())

def upload_recent_data_kaggle(date: str = os.getenv("RUN_DATE")):   
    try:
        if not date:
            logger.error("RUN_DATE environment variable is not set. Please provide a valid date in YYYY-MM-DD format.")
            raise ValueError("RUN_DATE environment variable is not set. Please provide a valid date in YYYY-MM-DD format.")
        
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid date format provided: {date}. Expected format is YYYY-MM-DD.")
            raise ValueError(f"Invalid date format provided: {date}. Expected format is YYYY-MM-DD.")
        
        with TemporaryDirectory() as temp_dir:
            for table in [config.I18N_COUNTRIES_TABLE_NAME,config.VIDEO_CATEGORIES_TABLE_NAME,config.VIDEO_STATS_TABLE_NAME,config.VIDEO_TABLE_NAME,config.COMMENT_TABLE_NAME]:   
                df = read_sql_tables(spark, table)
                write_csv_tables(df, table, temp_dir)
                logger.info(f"Successfully uploaded the recent data of {table} to Kaggle dataset: {config.KAGGLE_DATASET_NAME} for date: {date}")
                
            version_notes = f"Upload recent data for date: {date}"
            handle = f"{config.KAGGLE_USERNAME}/{config.KAGGLE_DATASET_NAME}"
            dataset_upload(
                handle,
                temp_dir,
                version_notes=version_notes
            )
            
    except Exception as e:
        logger.error(f"Error uploading recent data to Kaggle for date: {date}. Error details: {e}")
        raise
    
if __name__ == "__main__":
    upload_recent_data_kaggle()