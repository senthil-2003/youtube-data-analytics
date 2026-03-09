from pyspark.sql import SparkSession
import os

from src.utils.logger import get_logger
from src.utils.load_env import get_env
from src.data_warehouse.io_operations import check_table_exists, check_table_contents, read_data
from src.data_warehouse.create_tables import create_tables
from src.utils.common_utils import azure_link_builder

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

spark = SparkSession.builder\
    .appName("sql_data_conversion")\
    .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.0.33")\
    .getOrCreate()

read_obj = read_data(spark)

def to_sql_pipeline(date: str):
    
    failed_tables = []
    for i in [cred.VIDEO_CATEGORIES_TABLE_NAME, cred.VIDEO_TABLE_NAME, cred.VIDEO_STATS_TABLE_NAME, cred.COMMENT_TABLE_NAME, cred.I18N_COUNTRIES_TABLE_NAME]:
        if not check_table_exists(spark, i, cred.DB_NAME):
            logger.warning(f"Table does not exist: {i}")
            failed_tables.append(i)
            
    if failed_tables:
        logger.info(f"Creating missing tables: {failed_tables}")
        create_table_obj = create_tables(spark)
        
        create_table_obj.create_missing_tables(failed_tables)

    if not check_table_contents(spark, cred.VIDEO_CATEGORIES_TABLE_NAME, cred.DB_NAME):
        logger.warning(f"Table is empty: {cred.VIDEO_CATEGORIES_TABLE_NAME}")
        
        video_categories_delta_table_location = azure_link_builder(container_name=cred.CONTAINER_NAME, account_name=cred.AZURE_ACCOUNT_NAME, location= os.path.join(cred.PROCESSED_FOLDER_NAME,cred.PROCESSED_VIDEO_CATEGORIES_DELTA_TABLE_NAME)).replace("\\","/")
        video_categories_df = read_obj.read_delta_table(video_categories_delta_table_location)
        
        #writing logic
        
    if not check_table_contents(spark, cred.I18N_COUNTRIES_TABLE_NAME, cred.DB_NAME):
        logger.warning(f"Table is empty: {cred.I18N_COUNTRIES_TABLE_NAME}")
        
        i1_countries_delta_table_location = azure_link_builder(container_name=cred.CONTAINER_NAME, account_name=cred.AZURE_ACCOUNT_NAME, location= os.path.join(cred.PROCESSED_FOLDER_NAME,cred.PROCESSED_I1_COUNTRIES_DELTA_TABLE_NAME)).replace("\\","/")
        i1_countries_df = read_obj.read_delta_table(i1_countries_delta_table_location)
        
        #writing logic
        
    
        
        
        
            
