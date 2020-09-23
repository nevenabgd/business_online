# twitter crawls
time spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
	--py-files sparkcc.py \
    ./common_crawl_extract.py \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE crawl = 'CC-MAIN-2020-16' AND subset = 'warc' and (position('twitter' in url_host_name) != 0)" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/ \
    twitter \
    --num_output_partitions 10 \
    --output_format parquet

# Writing to S3 example
time spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
	--py-files sparkcc.py \
    ./common_crawl_extract.py \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE crawl = 'CC-MAIN-2020-16' AND subset = 'warc' AND (position('yelp' in url_host_name) != 0) limit 200000" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/ \
    yelp \
    --num_output_partitions 10 \
    --s3_output_path s3a://dataeng-bucket/crawlerdata/yelp_test

# fast execution / for testing
time spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
	--py-files sparkcc.py \
    ./common_crawl_extract.py \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE crawl = 'CC-MAIN-2020-16' AND subset = 'warc' AND content_languages='eng' AND url_host_tld = 'is' LIMIT 100" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/ \
    output7 \
    --num_output_partitions 1 \
    --s3_output_path s3a://dataeng-bucket/crawlerdata/test_fix



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