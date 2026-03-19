from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, MapType, BooleanType

class Schema:
    @staticmethod
    def get_comment_file_schema() -> StructType:
        comment_file_schema = StructType([
                                        StructField("items", 
                                            ArrayType(
                                                StructType([
                                                    StructField("snippet", 
                                                        StructType([
                                                            StructField("topLevelComment", 
                                                                StructType([
                                                                            StructField("id", StringType(), False),
                                                                            StructField("snippet", 
                                                                                StructType([
                                                                                    StructField("authorDisplayName", StringType(), False),
                                                                                    StructField("channelId", StringType(), False),
                                                                                    StructField("textOriginal", StringType(), False),
                                                                                    StructField("publishedAt", TimestampType(), False),
                                                                                    StructField("likeCount", IntegerType(), False)
                                                                                ]))
                                                                            ])),
                                                            StructField("totalReplyCount", IntegerType(), False),
                                                            StructField("videoId", StringType(), False)
                                                                    ]))
                                                        ]))
                                                )
                                    ])
        
        return comment_file_schema
    
    @staticmethod
    def get_video_file_schema() -> StructType:
        video_file_schema = StructType([
                                        StructField("items", ArrayType(
                                            StructType([
                                                StructField("id", StringType(), False),
                                                StructField("snippet", StructType([
                                                    StructField("categoryId", StringType(), False),
                                                    StructField("publishedAt", TimestampType(), False),
                                                    StructField("channelId", StringType(), False),
                                                    StructField("title", StringType(), False),

                                                    StructField("channelTitle", StringType(), False),
                                                    StructField("defaultLanguage", StringType(), False),
                                                    StructField("liveBroadcastContent", StringType(), True),
                                                    StructField("defaultAudioLanguage", StringType(), True)
                                                ]), True),

                                                StructField("contentDetails", StructType([
                                                    StructField("duration", StringType(), False),
                                                    StructField("definition", StringType(), False),
                                                    StructField("regionRestriction", StructType([
                                                        StructField("blocked", ArrayType(StringType()), True)
                                                    ]), True),
                                                    StructField("contentRating", MapType(StringType(),StringType()), True)
                                                ]), True),

                                                StructField("status", StructType([
                                                    StructField("madeForKids", BooleanType(), True)
                                                ]), True),

                                                StructField("statistics", StructType([
                                                    StructField("viewCount", StringType(), False),
                                                    StructField("likeCount", StringType(), False),
                                                    StructField("commentCount", StringType(), False)
                                                ]), True),

                                                StructField("paidProductPlacementDetails", StructType([
                                                    StructField("hasPaidProductPlacement", BooleanType(), True)
                                                ]), True),

                                            ])
                                        ), True)
                                    ])

        return video_file_schema
    
    @staticmethod
    def get_categories_schema() -> StructType:
        video_categories_schema = StructType([
                                    StructField("items", ArrayType(StructType([
                                        StructField("id",StringType(),False),
                                        StructField("snippet",StructType([
                                            StructField("title",StringType(),False)
                                        ]),False)
                                    ])),False)
                                    ])
        return video_categories_schema
    
    @staticmethod
    def get_i1_countries_schema() -> StructType:
        i1_country_schema = StructType([
                            StructField("items",ArrayType(
                                StructType([
                                StructField("snippet", StructType([
                                    StructField("gl",StringType(),False),
                                    StructField("name",StringType(),False)
                                ]),False)
                            ])
                            ),False)    
                            ])
        
        return i1_country_schema