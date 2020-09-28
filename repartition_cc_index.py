import argparse

from consts import CC_INDEX_S3_PATH, MY_S3_CRAWL_DATA_PATH, MY_S3_CRAWL_INDEX_PATH

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


class RepartitionCCIndex(object):

    name = "RepartitionCCIndex"

    DEFAULT_INDEX_QUERY=("SELECT abs(hash(url)) % 10 as bucket, url, warc_filename, warc_record_offset, warc_record_length" +
        " FROM ccindex WHERE subset = 'warc' AND content_languages='eng' " +
        "AND (position('news' in url_host_name) != 0)")

    def parse_arguments(self, script_name):
        """ Returns the parsed arguments from the command line """

        arg_parser = argparse.ArgumentParser(prog=script_name,
            description="Repartitioning cc index into 10 buckets",
            conflict_handler='resolve')
        arg_parser.add_argument("--crawl", type=str, required=True,
                                help='crawl')
        arg_parser.add_argument("--query", type=str, required=False, default=self.DEFAULT_INDEX_QUERY,
                                help="Index sql query")

        args = arg_parser.parse_args()
        return args
    
    def run(self):
        args = self.parse_arguments("repartition_cc_index.py")
        conf = SparkConf()
        sc = SparkContext(
            appName=self.name,
            conf=conf)
        spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

        crawl_partition_spec = "crawl={}".format(args.crawl)
        index_path = "{}/{}".format(CC_INDEX_S3_PATH, crawl_partition_spec)

        df = spark.read.load(index_path)
        df.createOrReplaceTempView("ccindex")

        sqldf = spark.sql(args.query)
        print("Executing query: {}".format(args.query))
        sqldf.explain()
        sqldf.show(10, False)
        sqldf.persist()
        num_rows = sqldf.count()
        print("Number of records/rows matched by query: {}".format(num_rows))

        output_path = "{}/{}".format(MY_S3_CRAWL_INDEX_PATH, crawl_partition_spec)
        sqldf.coalesce(10).write.partitionBy("bucket").mode("overwrite").parquet(output_path)

        sc.stop()

if __name__ == '__main__':
    repartitionIndex = RepartitionCCIndex()
    repartitionIndex.run()
  
