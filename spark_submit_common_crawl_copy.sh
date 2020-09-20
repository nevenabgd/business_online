spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
	--py-files sparkcc.py,word_count.py \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
    ./cc_index_word_count.py \
    --query "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex WHERE crawl = 'CC-MAIN-2020-24' AND subset = 'warc' AND url_host_tld = 'is' LIMIT 10" \
    s3a://commoncrawl/cc-index/table/cc-main/warc/ \
    myccindexwordcountoutput2 \
    --num_output_partitions 1 \
    --output_format json \
    --output_compression None