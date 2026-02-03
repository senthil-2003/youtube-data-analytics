from dotenv import load_dotenv
import os
import logging

load_dotenv('.env')
logger = logging.getLogger(__name__)

class get_env:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(get_env, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        try:
            if self._initialized:
                return

            self._initialized = True
            
            self.YOUTUBE_API_KEY = os.getenv('youtube_api_key', None)
            self.MAX_RESULTS_LIMIT = os.getenv('max_results_limit',None)
            self.MAX_RESULTS_LIMIT = int(self.MAX_RESULTS_LIMIT) if self.MAX_RESULTS_LIMIT is not None else None
                      
            self.STORAGE_CONNECTION_STRING = os.getenv('azure_storage_connection_string', None)
            self.LOG_CONNECTION_STRING = os.getenv('azure_log_connection_string', None)
            
            self.RAW_FOLDER_NAME = os.getenv('raw_folder_name', None)
            self.PROCESSED_FOLDER_NAME = os.getenv('processed_folder_name', None)
            self.UTIL_FOLDER_NAME = os.getenv('util_folder_name', None)
            
            self.I18N_FILE_NAME = os.getenv("i18n_file_name", None)
            self.CATEGORIES_FOLDER_NAME = os.getenv("categories_folder_name", None)
            self.CONTAINER_NAME = os.getenv("container_name",None)
            
            self.POPULAR_VIDEO_FILE_NAME = os.getenv("popular_video_file_name", None)
            self.POPULAR_COMMENTS_FILE_NAME = os.getenv("popular_comments_file_name", None)
            
            self.LOG_PATH = os.getenv('logs_destination', None)
            self.LOG_FILE_NAME = os.getenv('log_file_name', None)
            self.LOG_LEVEL = os.getenv('logging_level', None)
            self.validate()
            
        except Exception as e:
            logger.critical(f"There was an exception while retrieving the environment variable values and the error is {e}")
            raise e
    
    def validate(self):
        missing=[]
        
        for name,val in vars(self).items():
            if val is None:
                missing.append(name)
                
        if missing:
            logger.critical(f"Missing required environment variables: {', '.join(missing)}")
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
        else:
            logger.info("config env validated successfully")
            