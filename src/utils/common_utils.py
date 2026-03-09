from src.utils.logger import get_logger
from src.utils.load_env import get_env

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

def azure_link_builder(container_name: str, account_name: str, location: str) -> str:
    try:
        if not container_name or not account_name or not location:
            logger.error("Container name, account name and location must be provided to build the azure link")
            raise ValueError("Container name, account name and location must be provided to build the azure link")
        location = location.strip("/").replace("\\","/")
        link = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{location}/"
        return link
    except Exception as e:
        logger.critical(f"Error building Azure link with container: {container_name}, account: {account_name}, location: {location}. Error: {e}")
        raise RuntimeError(f"Error building Azure link with container: {container_name}, account: {account_name}, location: {location}. Error: {e}") from e