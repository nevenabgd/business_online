
# partitioned by (crawl)
CC_INDEX_S3_PATH="s3a://commoncrawl/cc-index/table/cc-main/warc"

MY_S3_ROOT_PATH="s3a://dataeng-bucket/businessonline"

# partitioned by (crawl, bucket)
MY_S3_CRAWL_INDEX_PATH="{}/{}".format(MY_S3_ROOT_PATH, "crawl_index")
MY_S3_CRAWL_DATA_PATH="{}/{}".format(MY_S3_ROOT_PATH, "crawl_data")

