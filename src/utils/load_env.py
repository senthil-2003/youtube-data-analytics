from dotenv import load_dotenv
import os

load_dotenv('.env')

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
            
            # youtube credentials
            self.YOUTUBE_API_KEY = os.getenv('youtube_api_key', None)
            self.MAX_RESULTS_LIMIT = os.getenv('max_results_limit',None)
            self.MAX_RESULTS_LIMIT = int(self.MAX_RESULTS_LIMIT) if self.MAX_RESULTS_LIMIT is not None else None

            # azure storage credentials
            self.STORAGE_CONNECTION_STRING = os.getenv('azure_storage_connection_string', None)
            self.LOG_CONNECTION_STRING = os.getenv('azure_log_connection_string', None)
            self.AZURE_ACCOUNT_NAME = os.getenv('account_name', None)
            self.AZURE_STORAGE_ACCOUNT_KEY = os.getenv('azure_storage_account_key', None)
            
            # folder names
            self.RAW_FOLDER_NAME = os.getenv('raw_folder_name', None)
            self.PROCESSED_FOLDER_NAME = os.getenv('processed_folder_name', None)
            self.UTIL_FOLDER_NAME = os.getenv('util_folder_name', None)
            
            # util file names
            self.I18N_FILE_NAME = os.getenv("i18n_file_name", None)
            self.CATEGORIES_FOLDER_NAME = os.getenv("categories_folder_name", None)
            self.CONTAINER_NAME = os.getenv("container_name",None)
            
            # raw data file names
            self.POPULAR_VIDEO_FILE_NAME = os.getenv("popular_video_file_name", None)
            self.POPULAR_COMMENTS_FILE_NAME = os.getenv("popular_comments_file_name", None)
            
            # delta table details
            self.PROCESSED_I18N_DELTA_TABLE_NAME = os.getenv("processed_i18n_delta_table_name", None)
            self.PROCESSED_VIDEO_CATEGORIES_DELTA_TABLE_NAME = os.getenv("processed_video_categories_delta_table_name", None)
            self.PROCESSED_VIDEO_DELTA_TABLE_NAME = os.getenv("processed_video_delta_table_name", None)
            self.PROCESSED_COMMENT_DELTA_TABLE_NAME = os.getenv("processed_comment_delta_table_name", None)
            
            # log related credentials
            self.LOG_PATH = os.getenv('logs_destination', None)
            self.LOG_FILE_NAME = os.getenv('log_file_name', None)
            self.LOG_LEVEL = os.getenv('logging_level', None)
            self.APPLICATION_LOG_NAME = os.getenv('app_logger_name', None)
            
            # database credentials
            self.VIDEO_CATEGORIES_TABLE_NAME = os.getenv('video_categories_table_name', None)
            self.VIDEO_TABLE_NAME = os.getenv('video_table_name', None)
            self.VIDEO_STATS_TABLE_NAME = os.getenv('video_stats_table_name', None)
            self.COMMENT_TABLE_NAME = os.getenv('comment_table_name', None)
            self.I18N_COUNTRIES_TABLE_NAME = os.getenv('i18n_countries_table_name', None)
            
            # database connection details
            self.DB_NAME = os.getenv('db_name', None)
            self.DB_HOSTNAME = os.getenv('db_host_name', None)
            self.DB_USER = os.getenv('db_user', None)
            self.DB_PASS = os.getenv('db_pass', None)
            self.DB_PORT = os.getenv('db_port', None)

            self.validate()
            
        except Exception as e:
            raise e
    
    def validate(self):
        missing=[]
        
        for name,val in vars(self).items():
            if name.startswith('_'):
                continue
            if val is None:
                missing.append(name)
                
        if missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
            