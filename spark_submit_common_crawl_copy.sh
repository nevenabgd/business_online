# Production
time spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
	--py-files sparkcc.py \
    ./common_crawl_extract.py \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE subset = 'warc' AND content_languages='eng'  AND (position('news' in url_host_name) != 0)" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2020-16/ \
    news \
    --num_output_partitions 200 \
    --s3_output_path s3a://dataeng-bucket/crawlerdata/news_2020_16


# fast execution / for testing
time spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
	--py-files sparkcc.py \
    ./common_crawl_extract.py \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE subset = 'warc' AND content_languages='eng' AND url_host_tld = 'is' LIMIT 100" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2020-16/ \
    news \
    --num_output_partitions 1 \
    --s3_output_path s3a://dataeng-bucket/crawlerdata/news_2020_16



#Jupiter example - index analysis
input_bucket = 's3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2020-16/'
df = spark.read.parquet(input_bucket)
df.createOrReplaceTempView("urls")
sqlDF = spark.sql("SELECT COUNT(*) as URLCount FROM urls")
sqlDF.show()


#Jupiter example - twitter data analysis
input_bucket = 's3a://dataeng-bucket/crawlerdata/twitter_all'
df = spark.read.parquet(input_bucket)
df.createOrReplaceTempView("data")
sqlDF = spark.sql("SELECT COUNT(*) FROM data")
sqlDF.show()