
# partitioned by (crawl)
CC_INDEX_S3_PATH="s3a://commoncrawl/cc-index/table/cc-main/warc"

MY_S3_ROOT_PATH="s3a://dataeng-bucket/businessonline"

# partitioned by (crawl, bucket)
MY_S3_CRAWL_INDEX_PATH="{}/{}".format(MY_S3_ROOT_PATH, "crawl_index")
MY_S3_CRAWL_DATA_PATH="{}/{}".format(MY_S3_ROOT_PATH, "crawl_data")

# company data
MY_S3_COMPANY_DATA_PATH="s3a://dataeng-bucket/company_data"

# cross joined data
MY_S3_CROSS_JOINED_DATA_PATH="{}/{}".format(MY_S3_ROOT_PATH, "cross_joined_data")

# aggregated data
MY_S3_MENTIONS_DATA_PATH="{}/{}".format(MY_S3_ROOT_PATH, "mentions")

#sentiment data
MY_S3_SENTIMENT_DATA_PATH = "{}/{}".format(MY_S3_ROOT_PATH, "sentiment")