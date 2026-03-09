from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from src.data_warehouse.access_azure_sql import conn
from src.utils.load_env import get_env
from src.utils.logger import get_logger

cred = get_env()
conn_obj = conn(
    host_name=cred.DB_HOSTNAME,
    database_name=cred.DB_NAME,
    username=cred.DB_USER,
    password=cred.DB_PASS
)
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

class read_data:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def read_delta_table(self, delta_table_location: str):
        try:
            delta_table_df = self.spark.read.format("delta").load(delta_table_location)
            logger.info(f"Successfully read delta table from location: {delta_table_location}")
            return delta_table_df
        except Exception as e:
            logger.error(f"Error reading delta table from location {delta_table_location}: {e}")
            raise
        
class write:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def write_sql_table(self):
        pass

def check_table_exists(spark: SparkSession, table_name: str, database_name: str) -> bool:
    if table_name.lower().strip() not in [cred.VIDEO_CATEGORIES_TABLE_NAME.lower(), cred.VIDEO_TABLE_NAME.lower(), cred.VIDEO_STATS_TABLE_NAME.lower(), cred.COMMENT_TABLE_NAME.lower(), cred.I18N_COUNTRIES_TABLE_NAME.lower()]:
        logger.error("Invalid table name provided.")
        raise ValueError("Invalid table name provided.")
    
    if database_name.lower().strip() != cred.DB_NAME.lower():
        logger.error("Invalid database name provided.")
        raise ValueError("Invalid database name provided.")
    
    jdbc_url = conn_obj.get_jdbc_url()
    connection_properties = conn_obj.get_connection_properties()
    
    query = f"""
    (SELECT COUNT(*) as count
    FROM information_schema.tables
    WHERE table_schema = '{database_name}' 
    AND table_name = '{table_name}') AS t
    """
    
    df = spark.read.jdbc(url = jdbc_url,
                         table=query,
                         properties=connection_properties)
    return df.first()[0] == 1

def check_table_contents(spark: SparkSession, table_name: str, database_name: str)-> bool:
    if table_name.lower().strip() not in [cred.VIDEO_CATEGORIES_TABLE_NAME.lower(), cred.VIDEO_TABLE_NAME.lower(), cred.VIDEO_STATS_TABLE_NAME.lower(), cred.COMMENT_TABLE_NAME.lower(), cred.I18N_COUNTRIES_TABLE_NAME.lower()]:
        logger.error("Invalid table name provided.")
        raise ValueError("Invalid table name provided.")
    
    if database_name.lower().strip() != cred.DB_NAME.lower():
        logger.error("Invalid database name provided.")
        raise ValueError("Invalid database name provided.")
    
    jdbc_url = conn_obj.get_jdbc_url()
    connection_properties = conn_obj.get_connection_properties()
    
    query = f"""
    select count(*) as count
    from {database_name}.{table_name}
    """
    
    df = spark.read.jdbc(url  = jdbc_url,
                         table = query,
                         properties = connection_properties)
    
    return df.first()[0]>0