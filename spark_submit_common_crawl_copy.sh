# Repartition cc index
time spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
    ./repartition_cc_index.py \
    --crawl "CC-MAIN-2020-34"

# Download cc data
time spark-submit \
    --executor-memory 1G \
    --num-executors 40 \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
    ./download_cc_data.py \
    --crawl "CC-MAIN-2020-34" \
    --bucket 3

# cross-join
time spark-submit \
    --executor-memory 5G \
    --num-executors 14 \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
    ./cross_join.py \
    --crawl "CC-MAIN-2020-34" \
    --bucket 1

# mentions
time spark-submit \
    --executor-memory 5G \
    --num-executors 14 \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
    ./mentions.py \
    --crawl "CC-MAIN-2020-34"

# sentiment
time spark-submit \
    --executor-memory 5G \
    --num-executors 14 \
    --packages org.apache.hadoop:hadoop-aws:3.2.0 \
    ./sentiment.py \
    --crawl "CC-MAIN-2020-34"

# Copy results from s3 to mysql
time spark-submit --packages org.apache.hadoop:hadoop-aws:3.2.0 \
    --executor-memory 1G \
    --num-executors 1 \
	./write_to_db.py \
	--crawl "CC-MAIN-2020-34" --endpoint main-db.cytnlabniy01.us-east-1.rds.amazonaws.com \
	--db test --user admin --password $DBPASS
	
python3 ./dashui.py \
	--endpoint main-db.cytnlabniy01.us-east-1.rds.amazonaws.com \
	--db test --user admin --password $DBPASS

# Connect to mysql command
mysql -u admin -p -h main-db.cytnlabniy01.us-east-1.rds.amazonaws.com


#Jupiter example - partition the index into 10 buckets
input_bucket = 's3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2020-16/'
df = spark.read.parquet(input_bucket)
df.createOrReplaceTempView("ccindex")
sqlDF = spark.sql("SELECT url, warc_filename, warc_record_offset, warc_record_length, hash(url) % 10 as bucket FROM ccindex WHERE subset = 'warc' AND content_languages='eng'  AND (position('news' in url_host_name) != 0) limit 20")
sqlDF.show(20, False)



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