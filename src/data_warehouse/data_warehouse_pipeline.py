from pyspark.sql import SparkSession
import os
from delta.tables import DeltaTable

from src.utils.logger import get_logger,setup_logger
from src.utils.load_env import get_env
from src.data_warehouse.io_operations import check_table_exists, check_table_contents, read_data, write_data
from src.utils.link_builder import azure_link_builder
from src.data_warehouse.join_operation import joinTables
from src.data_warehouse.access_azure_sql import conn

cred = get_env()

setup_logger(
    root_logger_name=cred.APPLICATION_LOG_NAME,
    log_path = cred.LOG_PATH,
    log_file_name = cred.LOG_FILE_NAME,
    log_level = cred.LOG_LEVEL,
    azure_connection_string = cred.LOG_CONNECTION_STRING
) 
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

spark = (
    SparkSession.builder
    .appName("sql_data_conversion")
    .config(
        "spark.jars.packages",
        ",".join([
            "io.delta:delta-spark_2.12:3.1.0",
            "com.mysql:mysql-connector-j:8.0.33",
            "org.apache.hadoop:hadoop-azure:3.3.4",
            "com.azure:azure-storage-blob:12.24.0"
        ])
    )
    .config(
        f"spark.hadoop.fs.azure.account.key.{cred.AZURE_ACCOUNT_NAME}.dfs.core.windows.net",
        cred.AZURE_STORAGE_ACCOUNT_KEY
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

conn_obj = conn(
    host_name=cred.DB_HOSTNAME,
    database_name=cred.DB_NAME,
    username=cred.DB_USER,
    password=cred.DB_PASS,
    port=cred.DB_PORT
)

read_obj = read_data(spark, conn_obj)
write_obj = write_data(spark, conn_obj)

def to_sql_pipeline(date: str = os.getenv("RUN_DATE")):
    
    # check for any missing tables in the database
    failed_tables = []
    for i in [cred.VIDEO_CATEGORIES_TABLE_NAME, cred.VIDEO_TABLE_NAME, cred.VIDEO_STATS_TABLE_NAME, cred.COMMENT_TABLE_NAME, cred.I18N_COUNTRIES_TABLE_NAME]:
        if not check_table_exists(spark, i, cred.DB_NAME, conn_obj):
            logger.warning(f"Table does not exist: {i}")
            failed_tables.append(i)
            
    if failed_tables:
        logger.info(f"Creating missing tables: {failed_tables}")
        raise RuntimeError(f"these tables are not found in the database: {', '.join(failed_tables)}. Please create these tables before running the pipeline.")
    else:
        logger.info("All required tables are present in the database. Proceeding with the pipeline.")

    # check if the i18n countries list is already present in the database, If not, then read the respective delta tbale and then write the data to the sql table.
    if not check_table_contents(spark, cred.I18N_COUNTRIES_TABLE_NAME, cred.DB_NAME, conn_obj):
        logger.warning(f"Table is empty: {cred.I18N_COUNTRIES_TABLE_NAME}")
        
        i1_countries_delta_table_location = azure_link_builder(container_name=cred.CONTAINER_NAME, account_name=cred.AZURE_ACCOUNT_NAME, location= os.path.join(cred.PROCESSED_FOLDER_NAME,cred.PROCESSED_I18N_DELTA_TABLE_NAME)).replace("\\","/")
        
        if not DeltaTable.isDeltaTable(spark, i1_countries_delta_table_location):
            logger.error(f"Delta table does not exist at location: {i1_countries_delta_table_location}")
            raise FileNotFoundError(f"Delta table does not exist at location: {i1_countries_delta_table_location}")
        
        i1_countries_df = read_obj.read_delta_table(i1_countries_delta_table_location)
        
        write_obj.write_sql_table(
            table_name=cred.I18N_COUNTRIES_TABLE_NAME,
            mode="append",
            dataframe=i1_countries_df
        )
        
        logger.info(f"Successfully wrote dataframe to table {cred.I18N_COUNTRIES_TABLE_NAME}")
    else:
        logger.info(f"Table {cred.I18N_COUNTRIES_TABLE_NAME} already has data. Skipping writing to this table.")
        
    # check if the video categories table is already populated with data, If not, then read the respective delta table and then write the data to the sql table.
    if not check_table_contents(spark, cred.VIDEO_CATEGORIES_TABLE_NAME, cred.DB_NAME, conn_obj):
        logger.warning(f"Table is empty: {cred.VIDEO_CATEGORIES_TABLE_NAME}")
        
        video_categories_delta_table_location = azure_link_builder(container_name=cred.CONTAINER_NAME, account_name=cred.AZURE_ACCOUNT_NAME, location= os.path.join(cred.PROCESSED_FOLDER_NAME,cred.PROCESSED_VIDEO_CATEGORIES_DELTA_TABLE_NAME)).replace("\\","/")
        
        if not DeltaTable.isDeltaTable(spark, video_categories_delta_table_location):
            logger.error(f"Delta table does not exist at location: {video_categories_delta_table_location}")
            raise FileNotFoundError(f"Delta table does not exist at location: {video_categories_delta_table_location}")
        
        video_categories_df = read_obj.read_delta_table(video_categories_delta_table_location)
        write_obj.write_sql_table(table_name=cred.VIDEO_CATEGORIES_TABLE_NAME,
                                  mode="append",
                                  dataframe=video_categories_df)
        logger.info(f"Successfully wrote dataframe to table {cred.VIDEO_CATEGORIES_TABLE_NAME}")
    else:
        logger.info(f"Table {cred.VIDEO_CATEGORIES_TABLE_NAME} already has data. Skipping writing to this table.")
    
    # check if the popular videos delta table present in the location.
    popular_video_delta_table_location = azure_link_builder(container_name=cred.CONTAINER_NAME, account_name=cred.AZURE_ACCOUNT_NAME, location= os.path.join(cred.PROCESSED_FOLDER_NAME,cred.PROCESSED_VIDEO_DELTA_TABLE_NAME)).replace("\\","/")
    if not DeltaTable.isDeltaTable(spark, popular_video_delta_table_location):
        logger.error(f"Delta table does not exist at location: {popular_video_delta_table_location}")
        raise FileNotFoundError(f"Delta table does not exist at location: {popular_video_delta_table_location}")
    
    # read the popular video delta table
    popular_video_delta_df = read_obj.read_delta_table(popular_video_delta_table_location)
    popular_video_delta_df = popular_video_delta_df.filter(popular_video_delta_df['ingestion_date_utc'] == date)

    # read the video categories sql table to get the category key.
    video_categories_sql_df = read_obj.read_sql_table(cred.VIDEO_CATEGORIES_TABLE_NAME)
    
    # join the popular video delta dataframe with the video categories sql table to get the category key
    popular_video_delta_df_with_category_key = joinTables(popular_video_delta_df, video_categories_sql_df, "left", ["category_id","region_code"]).drop("view_count").drop("like_count").drop("comment_count").drop("ingestion_date_utc").drop("ingestion_time_utc").drop("category_id").drop("region_code").drop("category_title")
    popular_video_delta_df_with_category_key = popular_video_delta_df_with_category_key.drop_duplicates(subset=["video_id"])
    existing_video_ids_sql_df = read_obj.read_sql_table(cred.VIDEO_TABLE_NAME).select("video_id")
    popular_video_delta_df_final = joinTables(popular_video_delta_df_with_category_key, existing_video_ids_sql_df, "left_anti", ["video_id"])
    
    write_obj.write_sql_table(table_name=cred.VIDEO_TABLE_NAME,
                              mode="append",
                              dataframe=popular_video_delta_df_final)
    logger.info(f"Successfully wrote dataframe to table {cred.VIDEO_TABLE_NAME}")
    
    # retrieve the video stats for each video record from main popular video delta dataframe and then write the data to the respective sql table.
    popular_video_stats_delta_df = popular_video_delta_df.select("view_count", "like_count", "comment_count", "region_code", "ingestion_date_utc", "ingestion_time_utc","video_id")
    
    #filtering existing data
    existing_video_stats_sql_df = read_obj.read_sql_table(cred.VIDEO_STATS_TABLE_NAME).select("video_id", "ingestion_date_utc","region_code")
    popular_video_stats_delta_df = joinTables(popular_video_stats_delta_df,existing_video_stats_sql_df, "left_anti", ["video_id","ingestion_date_utc","region_code"])

    write_obj.write_sql_table(table_name=cred.VIDEO_STATS_TABLE_NAME,
                              mode="append",
                              dataframe=popular_video_stats_delta_df)
    logger.info(f"Successfully wrote dataframe to table {cred.VIDEO_STATS_TABLE_NAME}")

    # check if the comments delta table preesent in the location.
    comments_delta_table_location = azure_link_builder(container_name=cred.CONTAINER_NAME,
                                                       account_name=cred.AZURE_ACCOUNT_NAME,
                                                       location= os.path.join(cred.PROCESSED_FOLDER_NAME,cred.PROCESSED_COMMENT_DELTA_TABLE_NAME)).replace("\\","/")
    if not DeltaTable.isDeltaTable(spark, comments_delta_table_location):
        logger.error(f"Delta table does not exist at location: {comments_delta_table_location}")
        raise FileNotFoundError(f"Delta table does not exist at location: {comments_delta_table_location}")
    
    # read the comments delta table
    comments_delta_df = read_obj.read_delta_table(comments_delta_table_location)
    comments_delta_df = comments_delta_df.filter(comments_delta_df['ingestion_date_utc'] == date)
    comments_delta_df_final = comments_delta_df.select("comment_id","author_display_name","content_channel_id","author_comment_original","published_date_utc","published_time_utc","like_count","total_replies_count", "video_id", "ingestion_date_utc","ingestion_time_utc")
   
    # filtering existing data
    existing_comments_sql_df = read_obj.read_sql_table(cred.COMMENT_TABLE_NAME).select("comment_id")
    comments_delta_df_final = joinTables(comments_delta_df_final,existing_comments_sql_df,"left_anti", ["comment_id"])
    
    write_obj.write_sql_table(table_name=cred.COMMENT_TABLE_NAME,
                                mode="append",
                                dataframe=comments_delta_df_final)
    logger.info(f"Successfully wrote dataframe to table {cred.COMMENT_TABLE_NAME}")
    logger.info("pipeline to convert delta table to sql table completed successfully.")
    
if __name__ == "__main__":
    to_sql_pipeline()
    
        
    
        
        
        
            
