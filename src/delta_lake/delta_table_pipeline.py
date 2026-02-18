from pyspark.sql import SparkSession
import os
from delta.tables import DeltaTable
from datetime import datetime
from typing import Optional

from src.utils.load_env import get_env
from src.delta_lake.io_operations import read_files, write_files
from src.delta_lake.selection import Format
from src.delta_lake.delta_lake_utils import check_file_exists, azure_link_builder
from src.utils.logger import get_logger
from src.utils.load_env import get_env

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

spark = (
    SparkSession.builder
    .appName("silver_data_conversion")

    # =========================
    # Delta Lake Configuration
    # =========================
    .config(
        "spark.jars.packages",
        ",".join([
            "io.delta:delta-spark_2.12:3.1.0",
            "org.apache.hadoop:hadoop-azure:3.3.4",
            "com.azure:azure-storage-blob:12.22.0"
        ])
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # =========================
    # ADLS Gen2 Configuration
    # =========================
    .config(
        f"spark.hadoop.fs.azure.account.key.{cred.AZURE_ACCOUNT_NAME}.dfs.core.windows.net",
        cred.AZURE_STORAGE_ACCOUNT_KEY
    )

    # Explicit ABFS filesystem
    .config(
        "spark.hadoop.fs.abfss.impl",
        "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem"
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.abfss.impl",
        "org.apache.hadoop.fs.azurebfs.Abfs"
    )

    # =========================
    # Performance
    # =========================
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")

    .getOrCreate()
)
        
cred = get_env()
read_obj = read_files(spark)
write_obj = write_files(spark)
df_filter_obj = Format()

def to_silver_pipeline(date: str)-> Optional[bool]:
    
    try:
        datetime.strptime(date, "%d-%m-%Y")
        logger.info(f"The date {date} is in the correct format (dd-mm-yyyy)")
    except ValueError:
        logger.error(f"The date {date} is not in the correct format (dd-mm-yyyy)")
        raise ValueError(f"The date {date} is not in the correct format (dd-mm-yyyy)")

    try: 
        i18n_delta_azure_location = azure_link_builder(cred.CONTAINER_NAME,cred.AZURE_ACCOUNT_NAME, os.path.join(cred.PROCESSED_FOLDER_NAME,cred.PROCESSED_I18N_DELTA_TABLE_NAME).replace("\\","/"))
        if not check_file_exists(spark, i18n_delta_azure_location):
            logger.info(f"The delta table for i18n data is not present in the provided location {i18n_delta_azure_location} and the pipeline will start processing the raw data to create the delta table")
            i1_8n_raw_azure_location = azure_link_builder(cred.CONTAINER_NAME,cred.AZURE_ACCOUNT_NAME, os.path.join(cred.UTIL_FOLDER_NAME,cred.I18N_FILE_NAME).replace("\\","/"))
            if not check_file_exists(spark, i1_8n_raw_azure_location):
                logger.error(f"The i18n json file name is not present in the provided location {i1_8n_raw_azure_location}")
                raise FileNotFoundError(f"The i18n json file name is not present in the provided location {i1_8n_raw_azure_location}")
            
            i1_8n_df_raw = read_obj.read_json(file_location=i1_8n_raw_azure_location, multiline_flag=True, type_toggle="i18n")
            i1_8n_df = df_filter_obj.format_i18n_region(i1_8n_df_raw)
            write_obj.to_delta_plain(i1_8n_df, i18n_delta_azure_location)  
            logger.info(f"The delta table for i18n data has been created successfully at the location {i18n_delta_azure_location}")
    except Exception as e:
        logger.error(f"An error occurred while processing the i18n data and the error is {e}")
        return None
    
    try:
        video_categories_delta_location = azure_link_builder(cred.CONTAINER_NAME,cred.AZURE_ACCOUNT_NAME,os.path.join(cred.PROCESSED_FOLDER_NAME,cred.PROCESSED_VIDEO_CATEGORIES_DELTA_TABLE_NAME).replace("\\","/"))
        if not check_file_exists(spark, video_categories_delta_location):
            logger.info(f"The delta table for video categories data is not present in the provided location {video_categories_delta_location} and the pipeline will start processing the raw data to create the delta table")
            video_categories_raw_azure_location = azure_link_builder(cred.CONTAINER_NAME,cred.AZURE_ACCOUNT_NAME, os.path.join(cred.UTIL_FOLDER_NAME,cred.CATEGORIES_FOLDER_NAME).replace("\\","/"))
            if not check_file_exists(spark, video_categories_raw_azure_location):
                logger.error(f"The video categories json file name is not present in the provided location {video_categories_raw_azure_location}")
                raise FileNotFoundError(f"The video categories json file name is not present in the provided location {video_categories_raw_azure_location}")

            video_categories_df_raw = read_obj.read_json(file_location=video_categories_raw_azure_location,multiline_flag=True, type_toggle="video_categories")
            video_categories_df = df_filter_obj.format_video_categories(video_categories_df_raw)
            write_obj.to_delta_plain(video_categories_df, video_categories_delta_location)
            logger.info(f"The delta table for video categories data has been created successfully at the location {video_categories_delta_location}")
    except Exception as e:
        logger.error(f"An error occurred while processing the video categories data and the error is {e}")
        return None
            
    try:
        popular_video_raw_data_location = azure_link_builder(cred.CONTAINER_NAME, cred.AZURE_ACCOUNT_NAME, os.path.join(cred.RAW_FOLDER_NAME, date, "*", cred.POPULAR_VIDEO_FILE_NAME).replace("\\","/"))
        popular_video_raw_df = read_obj.read_json(file_location=popular_video_raw_data_location, multiline_flag=True, type_toggle="video")
        popular_video_raw_df_filtered = df_filter_obj.format_videos(popular_video_raw_df)
        
        video_data_delta_location = azure_link_builder(cred.CONTAINER_NAME, cred.AZURE_ACCOUNT_NAME, os.path.join(cred.PROCESSED_FOLDER_NAME, cred.PROCESSED_VIDEO_DELTA_TABLE_NAME).replace("\\","/"))
        if DeltaTable.isDeltaTable(spark,video_data_delta_location):
            logger.debug(f"The delta table for video data is present at the location {video_data_delta_location}")

            write_obj.to_delta(df = popular_video_raw_df_filtered,
                               file_location = video_data_delta_location,
                               mode="append")
            
            logger.info(f"The delta table for video data has been appended successfully with the new data from the raw files")
        else:
            logger.debug(f"The delta table for video data is not present at the location {video_data_delta_location} and it will be created now")
            write_obj.to_delta(df = popular_video_raw_df_filtered, 
                               mode = "overwrite",
                               file_location = video_data_delta_location)
            
            logger.info(f"The delta table for video data has been created successfully at the location {video_data_delta_location}")
    except Exception as e:
        logger.error(f"An error occurred while processing the video data and the error is {e}")
        return None
    
    try:
        popular_comments_raw_data_location = azure_link_builder(cred.CONTAINER_NAME, cred.AZURE_ACCOUNT_NAME, os.path.join(cred.RAW_FOLDER_NAME, date, "*", cred.POPULAR_COMMENTS_FILE_NAME+"_*.json").replace("\\","/"))
        popular_comments_raw_df = read_obj.read_json(file_location=popular_comments_raw_data_location, multiline_flag=True, type_toggle="comment")
        popular_comments_raw_df_filtered = df_filter_obj.format_comments(popular_comments_raw_df)

        comment_data_delta_location = azure_link_builder(cred.CONTAINER_NAME, cred.AZURE_ACCOUNT_NAME, os.path.join(cred.PROCESSED_FOLDER_NAME, cred.PROCESSED_COMMENT_DELTA_TABLE_NAME).replace("\\","/"))
        if DeltaTable.isDeltaTable(spark, comment_data_delta_location):
            logger.debug(f"The delta table for comment data is present at the location {comment_data_delta_location}")
            write_obj.to_delta(df = popular_comments_raw_df_filtered, 
                               mode = "append", 
                               file_location = comment_data_delta_location)
            logger.info(f"The delta table for comment data has been appended successfully with the new data from the raw files")
        else:
            logger.debug(f"The delta table for comment data is not present at the location {comment_data_delta_location} and it will be created now")
            write_obj.to_delta(df = popular_comments_raw_df_filtered, 
                               mode = "overwrite", 
                               file_location = comment_data_delta_location)
            logger.info(f"The delta table for comment data has been created successfully at the location {comment_data_delta_location}")
    except Exception as e:
        logger.error(f"An error occurred while processing the comment data and the error is {e}")
        return None
    
    return True

if __name__ == "__main__":
    try:
        date = "07-11-2025"
        flag = to_silver_pipeline(date=date)
        if flag:
            logger.info(f"The silver pipeline has completed successfully for the date {date}")
        else:
            logger.warning(f"The silver pipeline has completed with issues for the date {date}")
    except Exception as e:
        logger.error(f"An error occurred while running the silver pipeline and the error is {e}")
        raise RuntimeError(f"An error occurred while running the silver pipeline and the error is {e}") from e
    finally:
        input("Press Enter to stop the Spark session...")
        spark.stop()
