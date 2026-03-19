from pyspark.sql import DataFrame

from src.utils.logger import get_logger
from src.utils.load_env import get_env

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

def joinTables(source_df: DataFrame, target_df: DataFrame, how: str, join_keys: list[str]) -> DataFrame:
    try:
        joined_df = source_df.join(
            other = target_df,
            on = join_keys,
            how=how
        )
        logger.info(f"Successfully joined the source dataframe with the target dataframe on keys: {join_keys}")
        return joined_df
    except Exception as e:
        logger.error(f"Error joining the source dataframe with the target dataframe on keys: {join_keys} - {e}")
        raise