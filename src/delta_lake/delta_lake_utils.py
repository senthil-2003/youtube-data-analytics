from py4j.java_gateway import java_import
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

from src.utils.logger import get_logger
from src.utils.load_env import get_env

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

def check_file_exists(spark: SparkSession, file_location: str) -> bool:
    try:
        java_import(spark._jvm,"org.apache.hadoop.fs.Path")
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

        path = spark._jvm.Path(file_location)
        exists = fs.exists(path)
        return exists
    
    except Exception as e:
        logger.error(f"Error checking file existence at {file_location}: {e}")
        return False

def azure_link_builder(container_name: str, account_name: str, location: str) -> str:
    try:
        if not container_name or not account_name or not location:
            logger.error("Container name, account name and location must be provided to build the azure link")
            raise ValueError("Container name, account name and location must be provided to build the azure link")
        
        link = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{location}"
        return link
    except Exception as e:
        logger.critical(f"Error building Azure link with container: {container_name}, account: {account_name}, location: {location}. Error: {e}")
        raise RuntimeError(f"Error building Azure link with container: {container_name}, account: {account_name}, location: {location}. Error: {e}") from e

def filter_excessive_rows(date: str, df: DataFrame) -> DataFrame:
    try:
        df_final = df.filter((col("ingestion_date") == date) | (col("most_popular_frequency") > 1))
        return df_final
    except Exception as e:
        logger.error(f"Error filtering excessive rows: {e}")
        raise RuntimeError(f"Error filtering excessive rows: {e}") from e