from pyspark.sql import SparkSession
from src.utils.load_env import get_env
from src.utils.logger import get_logger

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME, __name__)

class create_tables:
    def __init__(self, spark: SparkSession, database_name: str = cred.DB_NAME):
        self.spark = spark
        self.database_name = database_name

    def create_video_categories_table(self)-> bool:
        pass
    
    def create_video_table(self)-> bool:
        pass
    
    def create_video_stats_table(self)-> bool:
        pass
    
    def create_comment_table(self)-> bool:
        pass
    
    def create_i18n_countries_table(self)-> bool:
        pass
    
    def create_missing_tables(self, missing_tables: list[str])-> bool:
        try:
            for table in missing_tables:
                if table == cred.VIDEO_CATEGORIES_TABLE_NAME:
                    flag = self.create_video_categories_table()
                elif table == cred.VIDEO_TABLE_NAME:
                    flag = self.create_video_table()
                elif table == cred.VIDEO_STATS_TABLE_NAME:
                    flag = self.create_video_stats_table()
                elif table == cred.COMMENT_TABLE_NAME:
                    flag = self.create_comment_table()
                elif table == cred.I18N_COUNTRIES_TABLE_NAME:
                    flag = self.create_i18n_countries_table()
                else:
                    raise ValueError(f"Unknown table name: {table}")
                
                if not flag or flag is None:
                    raise ValueError(f"Failed to create table: {table}")
                
            logger.info(f"Successfully created missing tables: {missing_tables}")
            
        except Exception as e:
            logger.error(f"Error creating missing tables: {e}")
            raise
