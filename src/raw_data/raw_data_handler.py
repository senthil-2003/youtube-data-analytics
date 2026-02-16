# check for parallel processing
import os
from azure.storage.blob import ContainerClient
from datetime import datetime
from typing import Optional

from src.utils.load_env import get_env
from src.utils.logger import setup_logger, get_logger
from src.raw_data.data_retreival import get_data
from src.raw_data.data_lake import interact_adlsgen2

# --- loading import credentials ---
cred = get_env()
  
## youtube cred
API_KEY = cred.YOUTUBE_API_KEY
MAX_RESULT_LIMIT = cred.MAX_RESULTS_LIMIT

## azure cred
STORAGE_CONNECTION_STRING = cred.STORAGE_CONNECTION_STRING
LOG_CONNECTION_STRING = cred.LOG_CONNECTION_STRING
 
# Folder names and blob paths
RAW_FOLDER_NAME = cred.RAW_FOLDER_NAME
UTIL_FOLDER_NAME = cred.UTIL_FOLDER_NAME
I18N_FILE_NAME = cred.I18N_FILE_NAME
CATEGORIES_FOLDER_NAME = cred.CATEGORIES_FOLDER_NAME
POPULAR_VIDEO_FILE_NAME = cred.POPULAR_VIDEO_FILE_NAME
POPULAR_COMMENTS_FILE_NAME = cred.POPULAR_COMMENTS_FILE_NAME
CONTAINER_NAME = cred.CONTAINER_NAME

## logging cred
LOG_PATH = cred.LOG_PATH
LOG_FILE_NAME = cred.LOG_FILE_NAME
LOG_LEVEL = cred.LOG_LEVEL

# --- Setup logger ---
setup_logger(
            root_logger_name=cred.APPLICATION_LOG_NAME,
            log_path=LOG_PATH,
            log_file_name=LOG_FILE_NAME,
            log_level=LOG_LEVEL,
            azure_connection_string=LOG_CONNECTION_STRING)

# --- Get module-specific logger ---
logger = get_logger(cred.APPLICATION_LOG_NAME,__name__)

def upload_file(adls_obj: interact_adlsgen2, container_obj: ContainerClient, data: dict, fileName: str) -> bool:
    upload_flag = adls_obj.upload_blob(container_obj = container_obj,
                              data = data,
                              file_name = fileName)
    
    return upload_flag

def get_video_and_comment_data(azure_object: interact_adlsgen2, youtube_obj: get_data, containerObj: ContainerClient, max_result_limit: int, region_code: Optional[str], video_file_name: str, comments_file_name: str) -> bool:
    get_popular_video = youtube_obj.get_most_popular_videos(max_result_limit = max_result_limit,
                                                            region_code = region_code)
    
    date = datetime.today().strftime('%d-%m-%Y')
    root_folder_name = os.path.join(RAW_FOLDER_NAME, date, region_code if region_code else "global")
    file_name = os.path.join(root_folder_name,video_file_name)
    
    flag = upload_file(adls_obj = azure_object, 
                       container_obj = containerObj, 
                       data = get_popular_video, 
                       fileName = file_name)
    
    if flag:
        logger.debug(f"the popular video {file_name} has been uploaded to the azure container")
    
    for i,item in enumerate(get_popular_video["items"], start = 1):
        id = item["id"]
        try:
            get_comment = youtube_obj.get_top_comments(max_result_limit = max_result_limit,
                                                    video_id = id)
            
            if get_comment is not None:
                logger.debug(f"The popular comment for  video id {id} and video number {i} in the region {region_code if region_code else 'global'} is uploaded to the container")
            else:
                logger.warning(f"This specific video with video id {id} and video number {i} in the region {region_code if region_code else 'global'} has turned off their comment section")
                continue
            
            upload_comment_flag = upload_file(adls_obj = azure_object,
                                            container_obj = containerObj,
                                            data = get_comment,
                                            fileName = os.path.join(root_folder_name,comments_file_name + "_" + str(i) + ".json"))
            
            if not upload_comment_flag:
                logger.critical(f"There is some exception for uploading the comments for {id} and video number {i} in the region {region_code if region_code else 'global'}")
                continue
        
        except Exception as e:
            logger.error(f"This specific video with video id {id} and video number {i} in the region {region_code if region_code else 'global'} and the error is {e}")
            continue
        
    return True

def get_i18n_codes(i18n_data: dict) -> list[str]:
    result = []
    for i in i18n_data["items"]:
        result.append(i["id"])
        
    return result

def check_missing(existing_data: list, new_data: list) -> list[str]:
    missing = list(set(new_data) - set(existing_data))
    return missing

def get_video_categories(video_data: list[str]) -> list[str]:
    result=[]
    for i in video_data:
        val = i.split('/')[-1].split('.')[0]
        result.append(val)
    
    return result

def raw_data_collector():
    
    logger.info(" --------------------------  Application started  ----------------------------")
    
    # class instances to interact
    youtube_data = get_data(api_key = API_KEY,
                               max_result_limit = MAX_RESULT_LIMIT)
    azure_obj = interact_adlsgen2(STORAGE_CONNECTION_STRING)
    
    # container object
    container_obj = azure_obj.get_container_obj(container_name = CONTAINER_NAME)
    
    # check i18n file
    check_i1_flag = azure_obj.check_files_exists(container_name = CONTAINER_NAME,
                                blob_file_name = os.path.join(UTIL_FOLDER_NAME,I18N_FILE_NAME))
    
    new_i18n_regions_list = youtube_data.get_i18nregion_list()
    new_i18n_country_codes = get_i18n_codes(i18n_data = new_i18n_regions_list)
    
    i18n_file_path = os.path.join(UTIL_FOLDER_NAME,I18N_FILE_NAME)
    if not check_i1_flag:
        logger.info("No existing i18n country name file not found")
        i18n_upload_flag = upload_file(adls_obj = azure_obj,
                                        container_obj = container_obj,
                                        data = new_i18n_regions_list,
                                        fileName = i18n_file_path)
        
        if i18n_upload_flag:
            logger.info("uploaded the i18n country file to the azure container")
        
    else:
        existing_i18n_regions_list = azure_obj.get_stored_raw_data(container_obj = container_obj,
                                                                    blob_name = i18n_file_path)
        if existing_i18n_regions_list is None:
            logger.warning("The existing i18n country code file is not found in the azure container and the new i18n country code file will be uploaded")
        
        existing_i18n_country_codes = get_i18n_codes(i18n_data = existing_i18n_regions_list)
        missing_code = check_missing(existing_data = existing_i18n_country_codes, 
                                                      new_data = new_i18n_country_codes)
        if len(missing_code) > 0:
            logger.info(f"New i18n country is added or this country code file name is not found in the azure container {missing_code}")
            i18n_upload_flag = upload_file(adls_obj = azure_obj,
                                            container_obj = container_obj,
                                            data = new_i18n_regions_list,
                                            fileName = i18n_file_path
                                            )
            
            if i18n_upload_flag:
                logger.info("the new i18n country code or the missing file is uploaded to the cloud file")
        else:
            logger.info("all i18n country details is present")
            
    # check video category file
    video_category_file_path = os.path.join(UTIL_FOLDER_NAME,CATEGORIES_FOLDER_NAME)
    check_video_categories = azure_obj.check_files_exists(container_name = CONTAINER_NAME,
                                                         blob_file_name = video_category_file_path)
    
    if not check_video_categories:
        logger.info("The video category file is not present in the location.")
        
        for i in new_i18n_country_codes:
            vid_category = youtube_data.get_video_categories(region = i)
            upload_flag = upload_file(
                    adls_obj = azure_obj,
                    container_obj =  container_obj,
                    data = vid_category,
                    fileName = os.path.join(video_category_file_path, str(i) + ".json")
                )
            
            if upload_flag:
                logger.debug(f"the video category for {i} is uploaded")
        
        logger.info(f"video categories for all countries is uploaded")
    
    else:
        name_starts = video_category_file_path.replace("\\","/") + "/"
        existing_countries_vdc = azure_obj.list_all_files(container_obj = container_obj,
                                                          name_starts_with = name_starts)
        existing_countries = get_video_categories(existing_countries_vdc)
        
        missing_file = check_missing(existing_data = existing_countries,
                                     new_data = new_i18n_country_codes)

        if len(missing_file) > 0:
            logger.info(f"Video category for {missing_file} is missing and will be uploaded")
            
            for i in missing_file:
                vid_category = youtube_data.get_video_categories(region = i)
                upload_flag = upload_file(
                    adls_obj = azure_obj,
                    container_obj =  container_obj,
                    data = vid_category,
                    fileName = os.path.join(video_category_file_path, str(i) + ".json")
                )
                
                if upload_flag:
                    logger.debug(f"video category for {i} is uploaded")
            
            logger.info("All missing video categories files are uploaded!!")
            
        else:
            logger.info("The videocategories for all regions is already present")
    
    # get top video and comment data, global and all countries
    global_flag = get_video_and_comment_data(azure_object = azure_obj,
                                    youtube_obj = youtube_data,
                                    containerObj = container_obj,
                                    max_result_limit = MAX_RESULT_LIMIT,
                                    region_code = None,
                                    video_file_name = POPULAR_VIDEO_FILE_NAME,
                                    comments_file_name = POPULAR_COMMENTS_FILE_NAME)
    if global_flag:
        logger.info(f"successfully inserted the video and comments for global region")

    for code in new_i18n_country_codes:
        flag = get_video_and_comment_data(azure_object = azure_obj,
                                    youtube_obj = youtube_data,
                                    containerObj = container_obj,
                                    max_result_limit = MAX_RESULT_LIMIT,
                                    region_code = code,
                                    video_file_name = POPULAR_VIDEO_FILE_NAME,
                                    comments_file_name = POPULAR_COMMENTS_FILE_NAME)
        
        if flag:
            logger.info(f"successfully inserted the video and comments for {code}")
            
    logger.info(" ------------ Application ended successfully ----------------")
            
if __name__ == "__main__":
    raw_data_collector()