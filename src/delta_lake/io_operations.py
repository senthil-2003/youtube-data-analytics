from src.delta_lake.schema import Schema
from pyspark.sql import SparkSession
from pyspark.sql.connect.dataframe import DataFrame
from delta.tables import DeltaTable

from src.utils.logger import get_logger
from src.utils.load_env import get_env

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

class read_files:
    def __init__(self, spark: SparkSession):
        self.schema = Schema()
        self.spark = spark

    def read_json(self, file_location: str, multiline_flag: bool, type_toggle: str) -> DataFrame:
        try:
            if type_toggle.lower() == "i18n":
                schema = self.schema.get_i1_countries_schema()
            elif type_toggle.lower() == "video":
                schema = self.schema.get_video_file_schema()
            elif type_toggle.lower() == "comment":
                schema = self.schema.get_comment_file_schema()
            elif type_toggle.lower() == "video_categories":
                schema = self.schema.get_categories_schema()
            else:
                raise ValueError(f"The type toggle value provided is {type_toggle} and it is not a valid value.")
                
            df = self.spark.read.option("multiline",multiline_flag)\
                .schema(schema)\
                .json(file_location)
                
            return df
        except Exception as e:
            logger.error(f"Error reading JSON file from {file_location}: {e}")
            raise RuntimeError(f"An error occurred while reading the json file from the location {file_location} and the error is {e}")
    
    def read_delta(self, file_location: str) -> DataFrame:
        try:
            df = self.spark.read.format("delta").load(file_location)
            return df
        except Exception as e:
            logger.error(f"Error reading Delta file from {file_location}: {e}")
            raise RuntimeError(f"An error occurred while reading the delta file from the location {file_location} and the error is {e}")

class write_files:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def to_delta_plain(self, df: DataFrame, file_location: str):
        try:
            df.write.format("delta").mode("overwrite").save(file_location)
        except Exception as e:
            logger.error(f"Error writing Delta file to {file_location}: {e}")
            raise RuntimeError(f"An error occurred while writing the delta file to the location {file_location} and the error is {e}")
        
    def upsert_to_delta_comment(self, df: DataFrame, src_file_location: str):
        try:
            src_delta_table = DeltaTable.forPath(self.spark, src_file_location).alias("src")
            
            src_delta_table.merge(df.alias("trg"),
                                "src.comment_id = trg.comment_id")\
                                .whenMatchedUpdate(set={
                                    "src.Like_Count": "trg.Like_Count",
                                    "src.Total_Replies_Count": "trg.Total_Replies_Count",
                                    "src.most_popular_frequency": "src.most_popular_frequency + trg.most_popular_frequency"
                                }).whenNotMatchedInsertAll().execute()
                                
        except Exception as e:
            logger.error(f"Error upserting to Delta comment table at {src_file_location}: {e}")
            raise RuntimeError(f"An error occurred while upserting to the delta comment table at the location {src_file_location} and the error is {e}")
        
    def upsert_to_delta_video(self, df: DataFrame, src_file_location: str):
        try:
            src_delta_table = DeltaTable.forPath(self.spark, src_file_location).alias("src")

            src_delta_table.merge(df.alias("trg"),
                                  "src.video_id = trg.video_id")\
                .whenMatchedUpdate(set={
                    "src.View_Count": "trg.View_Count",
                    "src.Like_Count": "trg.Like_Count",
                    "src.Comment_Count": "trg.Comment_Count",
                                        "src.most_popular_frequency": "src.most_popular_frequency + trg.most_popular_frequency"
                                  }).whenNotMatchedInsertAll().execute()
                
        except Exception as e:
            logger.error(f"Error upserting to Delta video table at {src_file_location}: {e}")
            raise RuntimeError(f"An error occurred while upserting to the delta video table at the location {src_file_location} and the error is {e}")
