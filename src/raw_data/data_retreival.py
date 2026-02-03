from googleapiclient.discovery import build, Resource
from google.auth.exceptions import MutualTLSChannelError
from googleapiclient.errors import HttpError
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class get_data:
    def __init__(self, api_key: str, max_result_limit: int):
        self.API_KEY = api_key
        self.RETRY_LIMIT = max_result_limit
        self.youtube_obj = self.__build_connection()
        
    def __build_connection(self) -> Resource:
        try:
            obj = build('youtube', 'v3', num_retries = self.RETRY_LIMIT, developerKey = self.API_KEY)
            logger.info("The youtube data api is builded for further data retreival")
            return obj
        
        except MutualTLSChannelError as e:
            logger.error(f"There are problems setting up the mutual TLS channel error.Check the youtube api connection !! : {e}")
            raise ConnectionRefusedError("There are problems setting up the mutual TLS channel error.Check the youtube api connection !!") from e
        
        except Exception as e:
            logger.error(f"Exception occured while connecting to youtube api: {e}")
            raise ConnectionError("Exception occured while connecting to youtube api") from e
   
    def __execute_request(self, request) -> Optional[dict]:
        try:
            response = request.execute()
            return response
        
        except HttpError as e:
            logger.error(f"There is a issue while fetching the data from the youtube api {e} with status code {e.status_code} and with response message {e.resp}")
            if e.status_code == 403:
                logger.warning(f"forbidden request")
                return None
            else:
                raise RuntimeError(f"There is a a issue while fetching the data from the youtube api {e} with status code {e.status_code} and with response message {e.resp}")
    
    def get_top_comments(self, max_result_limit: int, video_id: str) -> Optional[dict]:
        request = self.youtube_obj.commentThreads().list(
            part = "snippet, id",
            order = "relevance",
            videoId = video_id,
            maxResults = max_result_limit,
            textFormat = "plainText"
        )
        
        result = self.__execute_request(request = request)
        if result is None:
            return None
        
        logger.debug(f"Top {max_result_limit} comments succesfully retrieved for the video id {video_id}")
        return result
    
    def get_most_popular_videos(self, max_result_limit: int, region_code: Optional[str]) -> Optional[dict]:
        request = self.youtube_obj.videos().list(
            part = "contentDetails, id, liveStreamingDetails, localizations, paidProductPlacementDetails, player, recordingDetails, snippet, statistics, status, topicDetails",
            chart = "mostPopular",
            maxResults = max_result_limit,
            regionCode = region_code
        )
        
        result = self.__execute_request(request = request)
        if result is None:
            return None
        
        logger.debug(f"Top {max_result_limit} popular videos is retrieved for the region_code {region_code if region_code is not None else 'global'} ")
        return result
    
    def get_video_categories(self, region: str) -> Optional[dict]:
        request = self.youtube_obj.videoCategories().list(
            part = "snippet",
            regionCode = region
        )
        
        result = self.__execute_request(request = request)
        
        if result is None:
            return None
        
        logger.debug(f"The video categories have been collected for the region {region}")
        return result
    
    def get_i18nregion_list(self) -> Optional[dict]:
        request = self.youtube_obj.i18nRegions().list(
            part = "snippet"
        )
        
        result = self.__execute_request(request = request)
        
        if result is None:
            return None
        
        logger.debug("I18n region list have been collected")
        return result
    
if __name__ == "__main__":
    from utils.load_env import get_env
    cred = get_env()
    API_KEY = cred.YOUTUBE_API_KEY
    MAX_RESULT_LIMIT = cred.MAX_RESULTS_LIMIT
    
    inst = get_data(api_key=API_KEY,
                    max_result_limit=MAX_RESULT_LIMIT)
    
    inst.get_i18nregion_list()
    