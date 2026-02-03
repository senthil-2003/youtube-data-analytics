youtube_analytics_project (resource group)
|
| ______ youtubedatalake (storage account name)
         |
         | ______ raw (container 1)
         |                      |
         |                      |________ DATE
         |                                  |
         |                                  |________ IN
         |                                  |        |
         |                                  |        |________ popular_videos.json
         |                                  |        |
         |                                  |        |________ popular_comments_1.json
         |                                  |        |
         |                                  |        |________ popular_comments_2.json
         |                                  |        |
         |                                  |        |________ popular_comments_3.json
         |                                  |        |
         |                                  |        |________ popular_comments_4.json
         |                                  |        |
         |                                  |        |________ popular_comments_5.json
         |                                  |
         |                                  |________ US
         |                                  |         |
         |                                  |         |________ popular_videos.json
         |                                  |         |
         |                                  |         |________ popular_comments_1.json
         |                                  |         |
         |                                  |         |________ popular_comments_2.json
         |                                  |         |
         |                                  |         |________ popular_comments_3.json
         |                                  |         |
         |                                  |         |________ popular_comments_4.json
         |                                  |         |
         |                                  |        |________ popular_comments_5.json
         |                                  |
         |                                  |________ SL
         |                       ........
         |
         | ______ utils (container 2)
         |                      |
         |                      |________ i18nregions_list.json
         |                      |
         |                      |________ video_categories
         |                                             |
         |                                             |_______ IN.json
         |                                             |
         |                                             |_______ US.json
         |                                             ......
         |
         | ______ processed (container 3)
         |                      |
         |                      |________ DATE
         |                                  |
         |                                  |________ IN
         |                                  |        |
         |                                  |        |________ popular_videos.json
         |                                  |        |
         |                                  |        |________ popular_comments_1.json
         |                                  |        |
         |                                  |        |________ popular_comments_2.json
         |                                  |        |
         |                                  |        |________ popular_comments_3.json
         |                                  |        |
         |                                  |        |________ popular_comments_4.json
         |                                  |        |
         |                                  |        |________ popular_comments_5.json
         |                                  |
         |                                  |________ US
         |                                  |         |
         |                                  |         |________ popular_videos.json
         |                                  |         |
         |                                  |         |________ popular_comments_1.json
         |                                  |         |
         |                                  |         |________ popular_comments_2.json
         |                                  |         |
         |                                  |         |________ popular_comments_3.json
         |                                  |         |
         |                                  |         |________ popular_comments_4.json
         |                                  |         |
         |                                  |        |________ popular_comments_5.json
         |                                  |
         |                                  |________ SL
         |                       ........