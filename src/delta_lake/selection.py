from pyspark.sql.functions import col, explode, input_file_name, regexp_replace, element_at, split, map_keys, map_values, expr, current_timestamp, date_format, current_date
from pyspark.sql.dataframe import DataFrame

from src.utils.logger import get_logger
from src.utils.load_env import get_env

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

class Format:
    @staticmethod
    def format_video_categories(df: DataFrame) -> DataFrame:
        try:
            df_selected = df.withColumn("items_exploded", explode(col("items"))).withColumn("region_code", element_at(split(input_file_name(),'/'),-1))
            df_filtered = df_selected.select(
                col("items_exploded.id").alias("category_id"),
                col("items_exploded.snippet.title").alias("category_title"),
                element_at(split(col("region_code"),"."),0).alias("region_code")
            )
            return df_filtered
        
        except Exception as e:
            logger.error(f"Error formatting video categories data: {e}")
            raise RuntimeError(f"An error occurred while formatting the video categories data and the error is {e}") from e
    
    @staticmethod
    def format_i18n_region(df: DataFrame) -> DataFrame:
        try:
            df_filtered = df.withColumn("items_exploded", explode(col("items")))\
                          .select(
                              col("items_exploded.snippet.gl").alias("region_code"),
                              col("items_exploded.snippet.name").alias("region_name")
                          )
            return df_filtered
        except Exception as e:
            logger.error(f"Error formatting i18n region data: {e}")
            raise RuntimeError(f"An error occurred while formatting the i18n region data and the error is {e}") from e

    @staticmethod
    def format_comments(df: DataFrame) -> DataFrame:
        try:
            df_filtered = df.withColumn('Items',explode(col('Items'))).withColumn("region_code", element_at(split(input_file_name(),'/'),-2))\
                .select(
                col("Items.snippet.topLevelComment.id").alias('Comment_ID'),
                regexp_replace(col("Items.snippet.topLevelComment.snippet.authorDisplayName"), "^@", "").alias('Author_Display_Name'),
                col("Items.snippet.topLevelComment.snippet.channelId").alias('Content_Channel_ID'),
                col("Items.snippet.topLevelComment.snippet.textOriginal").alias('Author_Comment_Original'),
                col("Items.snippet.topLevelComment.snippet.publishedAt").alias('Published_Time'),
                col("Items.snippet.topLevelComment.snippet.likeCount").alias('Like_Count'),
                col("Items.snippet.totalReplyCount").alias('Total_Replies_Count'),
                col("Items.snippet.videoId").alias("video_id"),
                col("region_code")
            )

            df_time_added = df_filtered.withColumn("Ingestion_Time",date_format(current_timestamp(), "HH:mm:ss")).withColumn("Ingestion_Date",current_date())
            return df_time_added
        
        except Exception as e:
            logger.error(f"Error formatting comments data: {e}")
            raise RuntimeError(f"An error occurred while formatting the comments data and the error is {e}") from e

    @staticmethod
    def format_videos(df: DataFrame) -> DataFrame:
        try:
            df_filtered = df.withColumn('items_exploded', explode(col('items'))).withColumn("path", input_file_name())\
                          .select(
                                    col("items_exploded.id").alias("video_id"),
                                    col("items_exploded.snippet.categoryId").alias("CategoryId"),
                                    col("items_exploded.snippet.publishedAt").alias("Published_Time"),
                                    col("items_exploded.snippet.channelId").alias("Channel_ID"),
                                    col("items_exploded.snippet.title").alias("Video_title"),
                                    col("items_exploded.snippet.channelTitle").alias("Channel_Name"),
                                    col("items_exploded.snippet.defaultLanguage").alias("Default_Language"),
                                    col("items_exploded.snippet.liveBroadcastContent").alias("Live_Broadcast_Content"),
                                    col("items_exploded.snippet.defaultAudioLanguage").alias("Default_Audio_Language"),
                                    col("items_exploded.contentDetails.duration").alias("Content_Duration"),
                                    col("items_exploded.contentDetails.definition").alias("Content_Definition"),
                                    col("items_exploded.status.madeForKids").alias("Content_Made_For_Kids"),
                                    col("items_exploded.statistics.viewCount").cast("long").alias("View_Count"),
                                    col("items_exploded.statistics.likeCount").cast("long").alias("Like_Count"),
                                    col("items_exploded.statistics.commentCount").alias("Comment_Count").cast("long"),
                                    col("items_exploded.paidProductPlacementDetails.hasPaidProductPlacement").alias("Paid_Product_Placement"),
                                    element_at(map_keys(col("items_exploded.contentDetails.contentRating")),1).alias("rating_type"),
                                    element_at(map_values(col("items_exploded.contentDetails.contentRating")),1).alias("rating_value"),
                                    element_at(split(col("path"),'/'),-2).alias("region_code")
                        )
                      
            df_normalized = df_filtered.withColumn("Content_Duration_seconds", 
                                                expr("""
                                                            coalesce(try_cast(regexp_extract(Content_Duration, '(?i)([0-9]+)D', 1) AS BIGINT), 0) * 86400 +
                                                            coalesce(try_cast(regexp_extract(Content_Duration, '(?i)T([0-9]+)H', 1) AS BIGINT), 0) * 3600 +
                                                            coalesce(try_cast(regexp_extract(Content_Duration, '(?i)T(?:[0-9]+H)?([0-9]+)M', 1) AS BIGINT), 0) * 60 +
                                                            coalesce(try_cast(regexp_extract(Content_Duration, '(?i)T(?:[0-9]+H)?(?:[0-9]+M)?([0-9]+)S', 1) AS BIGINT), 0)
                                                        """)).drop("Content_Duration")

            df_time_added = df_normalized.withColumn("Ingestion_Date",current_date()).withColumn("Ingestion_Time",date_format(current_timestamp(), "HH:mm:ss"))

            return df_time_added
        
        except Exception as e:
            logger.error(f"Error formatting videos data: {e}")
            raise RuntimeError(f"An error occurred while formatting the videos data and the error is {e}") from e