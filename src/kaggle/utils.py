from pyspark.sql import SparkSession, DataFrame
from glob import glob
import shutil

from src.utils.load_env import get_env
from src.utils.logger import get_logger

config = get_env()
logger = get_logger(config.APPLICATION_LOG_NAME, __name__)

class db_cred():
    def __init__(self):
        self.jdbc_url = f"jdbc:mysql://{config.DB_HOSTNAME}:{config.DB_PORT}/{config.DB_NAME}?useSSL=true"
        self.connection_properties = {
            "user": config.DB_USER,
            "password": config.DB_PASS,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

def read_sql_tables(spark: SparkSession, table_name: str) -> DataFrame:
    try:
        if table_name not in [config.VIDEO_CATEGORIES_TABLE_NAME, config.I18N_COUNTRIES_TABLE_NAME, config.VIDEO_TABLE_NAME, config.VIDEO_STATS_TABLE_NAME, config.COMMENT_TABLE_NAME]:
            logger.error(f"Invalid table name: {table_name} provided for reading the sql table.")
            raise ValueError(f"Invalid table name: {table_name}")
        
        db_credentials = db_cred()
        sql_df = spark.read.format("jdbc")\
        .option("url",db_credentials.jdbc_url)\
        .option("dbtable", f"{config.DB_NAME}.{table_name}")\
        .options(**db_credentials.connection_properties)\
        .load()
        
        logger.info(f"Successfully read SQL table: {table_name}")
        return sql_df
    except Exception as e:
        logger.info(f"Error reading SQL table {table_name}: {e}")
        raise
    
def write_csv_tables(df: DataFrame, table_name: str, temp_path: str):
    try:
        if table_name not in [config.VIDEO_CATEGORIES_TABLE_NAME, config.I18N_COUNTRIES_TABLE_NAME, config.VIDEO_TABLE_NAME, config.VIDEO_STATS_TABLE_NAME, config.COMMENT_TABLE_NAME]:
            logger.error(f"Invalid table name: {table_name} provided for writing the sql table.")
            raise ValueError(f"Invalid table name: {table_name}")
        
        staging_path = f"{temp_path}/spark_tmp_{table_name}.csv"
        
        df.coalesce(1).write.format("csv")\
        .option("header", "true")\
        .mode("overwrite")\
        .save(staging_path)
        
        part_file = glob(f"{staging_path}/part-*.csv")[0]
        
        if not part_file:
            logger.error(f"No part file found in the staging path: {staging_path} for table: {table_name}")
            raise FileNotFoundError(f"No part file found in the staging path: {staging_path} for table: {table_name}")
        
        shutil.move(part_file,f"{temp_path}/{table_name}.csv")
        shutil.rmtree(staging_path)
        
        logger.info(f"Successfully wrote the dataframe to csv for table: {table_name} at path: {temp_path}/{table_name}.csv")
        
    except Exception as e:
        logger.error(f"Error writing dataframe to csv for table: {table_name} at path: {temp_path}/{table_name}.csv. Error details: {e}")
        raise
