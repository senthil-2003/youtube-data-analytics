from azure.storage.blob import BlobServiceClient, ContainerClient
import json

from src.utils.logger import get_logger
from src.utils.load_env import get_env

cred = get_env()
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

class interact_adlsgen2:
    def __init__(self, storage_connection_cred: str):
        self.STORAGE_CONNECTION_STRING = storage_connection_cred
        self.SERVICE_CLIENT = self.__build_connection()
        
    def __build_connection(self) -> BlobServiceClient:
        try:
            conn = BlobServiceClient.from_connection_string(self.STORAGE_CONNECTION_STRING)
            logger.info(f"The connection has been made to azure container")
            return conn
        
        except Exception as e:
            logger.error(f"There is an exception while building connection to the specified storage account because {e}")
            raise RuntimeError(f"There is an exception while building connection to the specified storage account because {e}")
        
    def get_container_obj(self, container_name: str) -> ContainerClient:
        try:
            container_obj = self.SERVICE_CLIENT.get_container_client(container_name)
            if not container_obj.exists():
                logger.debug("given container is not already present. creating it")
                self.SERVICE_CLIENT.create_container(name = container_name)
            
            logger.info("Container found and sending the object to interact with it")
            return container_obj
            
        except Exception as e:
            logger.error(f"There is an exception while building connection to the specified container because {e}")
            raise RuntimeError(f"There is an exception while building connection to the specified container because {e}")
        
    def check_files_exists(self, container_name: str, blob_file_name: str) -> bool:
        try:
            blob_obj = self.SERVICE_CLIENT.get_blob_client(container= container_name,
                                                        blob = blob_file_name)
            
            if not blob_obj.exists():
                logger.warning(f"The util file {blob_file_name} is not present in the given container {container_name}")
                return False
            else:
                logger.info(f"The util file {blob_file_name} is present in the given container {container_name}")
                return True
            
        except Exception as e:
            logger.error(f"There is an exception while creating a connection to the specifiec blob becuase {e}")
            raise RuntimeError(f"There is an exception while creating a connection to the specifiec blob becuase {e}")
        
    def upload_blob(self, container_obj: ContainerClient, data: dict, file_name: str) -> bool:
        try:
            blob_client = container_obj.get_blob_client(file_name)
            
            if isinstance(data, dict):
                data = json.dumps(data, indent = 2)

            blob_client.upload_blob(data = data,
                                    overwrite = True)
            
            return True
        
        except Exception as e:
            logger.error(f"There is an exception while uploading {file_name} to the container {e}")
            raise RuntimeError(f"There is an exception while uploading {file_name} to the container {e}")
        
    def list_all_files(self, container_obj: ContainerClient, name_starts_with: str) -> list[str]:
        try:
            list_blob = container_obj.list_blobs(name_starts_with = name_starts_with)
            
            file_name = [i.name for i in list_blob]
            
            logger.info(f"List of {len(file_name)} files present in the container with the name starts with {name_starts_with}")
            return file_name
        
        except Exception as e:
            logger.error(f"There is an exception while listing all the objects in the specified container that starts with {name_starts_with} and the exception is {e}")
            raise RuntimeError(f"There is an exception while listing all the objects in the specified container that starts with {name_starts_with} and the exception is {e}")
        
    def get_stored_raw_data(self, container_obj: ContainerClient, blob_name: str) -> dict:
        try:
            blob_client = container_obj.get_blob_client(blob = blob_name)
            
            if not blob_client.exists():
                logger.warning(f"The blob {blob_name} is not present in the given container")
                return None
            
            blob_data = blob_client.download_blob().readall()
            blob_utf = blob_data.decode('utf-8')
            json_data = json.loads(blob_utf)
            
            logger.info("i18n country codes with their details is received and will be sent")
            return json_data
        
        except Exception as e:
            logger.error(f"Error reading the blob {blob_name}, because {e}")
            raise RuntimeError(f"There is an exception while listing all the objects in the specified container {e}")
