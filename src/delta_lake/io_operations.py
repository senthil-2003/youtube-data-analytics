from src.delta_lake.schema import Schema
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
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
        
    def to_delta(self, df: DataFrame, categories: str, mode: str, file_location: str):
        if mode not in ["append", "overwrite", "ignore", "errorifexists"]:
            logger.error(f"Invalid write mode for delta table: {mode}.")
            raise ValueError(f"Invalid write mode for delta table: {mode}.")
        
        if categories not in ("i18n", "video", "comment", "categories"):
            logger.error(f"Invalid category for writing delta table: {categories}.")
            raise ValueError(f"Invalid category for writing delta table: {categories}.")
        
        try:
            if categories == "video" or categories == "comment":
                df.write.format("delta").mode(mode).partitionBy("Ingestion_Date").save(file_location)
            else:
                df.write.format("delta").mode(mode).save(file_location)
        except Exception as e:
            logger.error(f"Error writing Delta file to {file_location}: {e}")
            raise RuntimeError(f"An error occurred while writing the delta file to the location {file_location} and the error is {e}")
        
    def merge(self, df: DataFrame, categories: str, old_delta_table_path: str):
        old_delta = DeltaTable.forPath(self.spark, old_delta_table_path).alias("src")

        if categories == "video":
            merge_condition = "src.video_id = trg.video_id AND src.region_code = trg.region_code AND src.Ingestion_Date = trg.Ingestion_Date"
        elif categories == "comment":
            merge_condition = "src.Comment_ID = trg.Comment_ID AND src.video_id = trg.video_id AND src.region_code = trg.region_code AND src.Ingestion_Date = trg.Ingestion_Date"
        else:
            logger.error(f"Invalid category for merging delta table: {categories}.")
            raise ValueError(f"Invalid category for merging delta table: {categories}.")

        old_delta.merge(
            df.alias("trg"),
            merge_condition
        ).whenNotMatchedInsertAll().execute()