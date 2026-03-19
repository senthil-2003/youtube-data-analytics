from pyspark.sql import SparkSession, DataFrame
from src.data_warehouse.access_azure_sql import conn
from src.utils.load_env import get_env
from src.utils.logger import get_logger

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

class read_data:
    def __init__(self, spark: SparkSession, conn_obj: conn):
        self.spark = spark
        self.connection_properties = conn_obj.get_connection_properties()
        self.jdbc_url = conn_obj.get_jdbc_url()
        self.valid_table_names = [cred.VIDEO_CATEGORIES_TABLE_NAME.lower(), cred.VIDEO_TABLE_NAME.lower(), cred.VIDEO_STATS_TABLE_NAME.lower(), cred.COMMENT_TABLE_NAME.lower(), cred.I18N_COUNTRIES_TABLE_NAME.lower()]
        
    def read_delta_table(self, delta_table_location: str) -> DataFrame:
        try:
            delta_table_df = self.spark.read.format("delta").load(delta_table_location)
            logger.info(f"Successfully read delta table from location: {delta_table_location}")
            return delta_table_df
        except Exception as e:
            logger.error(f"Error reading delta table from location {delta_table_location}: {e}")
            raise
        
    def read_sql_table(self, table_name: str, database_name: str = cred.DB_NAME) -> DataFrame:
        try: 
            if table_name.strip().lower() not in self.valid_table_names:
                logger.error(f"Invalid table name - {table_name} provided for reading the sql table.")
                raise ValueError(f"Invalid table name - {table_name} provided for reading the sql table.")
                       
            sql_table_df = self.spark.read.format("jdbc")\
                           .option("url", self.jdbc_url)\
                           .options(**self.connection_properties)\
                           .option("dbtable", f"{database_name}.{table_name}")\
                           .load()
            logger.info(f"Successfully read sql table: {table_name} from database: {database_name}")
            return sql_table_df
        
        except Exception as e:
            logger.error(f"Error reading sql table {table_name} from database {database_name}: {e}")
            raise
            
class write_data:
    def __init__(self, spark: SparkSession, conn_obj: conn):
        self.spark = spark
        self.jdbc_url = conn_obj.get_jdbc_url()
        self.connection_properties = conn_obj.get_connection_properties()
        self.valid_table_names = [cred.VIDEO_CATEGORIES_TABLE_NAME.lower(), cred.VIDEO_TABLE_NAME.lower(), cred.VIDEO_STATS_TABLE_NAME.lower(), cred.COMMENT_TABLE_NAME.lower(), cred.I18N_COUNTRIES_TABLE_NAME.lower()]
        self.valid_modes = ["append", "overwrite", "ignore", "error"]
        
    def write_sql_table(self, table_name: str, mode : str, dataframe: DataFrame, database_name: str = cred.DB_NAME):
        if table_name.strip().lower() not in self.valid_table_names:
            logger.error(f"Invalid table name - {table_name} provided for writing the dataframe.")
            raise ValueError(f"Invalid table name - {table_name} provided for writing the dataframe.")

        if mode.lower().strip() not in self.valid_modes:
            logger.error(f"Invalid table writing mode - {mode} provided for writing the dataframe")
            raise ValueError(f"Invalid table writing mode - {mode} provided for writing the dataframe")

        write_batch_size = 5000
        num_partitions = 4
        
        try:
            dataframe.write.format("JDBC")\
                .option("url",self.jdbc_url)\
                .options(**self.connection_properties)\
                .option("dbtable",f"{database_name}.{table_name}")\
                .option("batchsize", write_batch_size)\
                .option("numPartitions", num_partitions)\
                .mode(mode)\
                .save()
        except Exception as e:
            logger.error(f"Error writing dataframe to table {table_name}: {e}")
            raise

def check_table_exists(spark: SparkSession, table_name: str, database_name: str, conn_obj: conn) -> bool:
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

def check_table_contents(spark: SparkSession, table_name: str, database_name: str, conn_obj: conn) -> bool:
    if table_name.lower().strip() not in [cred.VIDEO_CATEGORIES_TABLE_NAME.lower(), cred.VIDEO_TABLE_NAME.lower(), cred.VIDEO_STATS_TABLE_NAME.lower(), cred.COMMENT_TABLE_NAME.lower(), cred.I18N_COUNTRIES_TABLE_NAME.lower()]:
        logger.error("Invalid table name provided.")
        raise ValueError("Invalid table name provided.")
    
    if database_name.lower().strip() != cred.DB_NAME.lower():
        logger.error("Invalid database name provided.")
        raise ValueError("Invalid database name provided.")
    
    jdbc_url = conn_obj.get_jdbc_url()
    connection_properties = conn_obj.get_connection_properties()
    
    query = f"""
    (select count(*) as count
    from {database_name}.{table_name}) as t
    """
    
    df = spark.read.jdbc(url  = jdbc_url,
                         table = query,
                         properties = connection_properties)
    
    return df.first()[0] > 0