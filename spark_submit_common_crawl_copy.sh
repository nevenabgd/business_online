# basic example
time spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
	--py-files sparkcc.py \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
    ./common_crawl_extract.py \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE crawl = 'CC-MAIN-2020-16' AND subset = 'warc' and (position('twitter' in url_host_name) != 0) LIMIT 100" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/ \
    output7 \
    --num_output_partitions 1 \
    --output_format json \
    --output_compression None

# Writing to S3 example
time spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
	--py-files sparkcc.py \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
    ./common_crawl_extract.py \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE crawl = 'CC-MAIN-2020-16' AND subset = 'warc' and (position('twitter' in url_host_name) != 0) LIMIT 100" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/ \
    output7 \
    --num_output_partitions 1 \
    --s3_output_path s3a://dataeng-bucket/crawlerdata/twitter \
	--fs_s3a_access_key $ACCESS_KEY \
	--fs_s3a_secret_key $SECRET_KEY

# fast execution / for testing
time spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
	--py-files sparkcc.py \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
	--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	--conf spark.hadoop.fs.s3a.access.key=$ACCESS_KEY \
	--conf spark.hadoop.fs.s3a.secret.key=$SECRET_KEY \
    ./common_crawl_extract.py \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE crawl = 'CC-MAIN-2020-16' AND subset = 'warc' AND url_host_tld = 'is' LIMIT 100" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/ \
    output7 \
    --num_output_partitions 1 \
    --s3_output_path s3a://dataeng-bucket/crawlerdata/twitter

