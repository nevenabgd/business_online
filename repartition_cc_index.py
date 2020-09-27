import argparse

from consts import CC_INDEX_S3_PATH, MY_S3_CRAWL_DATA_PATH, MY_S3_CRAWL_INDEX_PATH

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


class RepartitionCCIndex(object):

    name = "RepartitionCCIndex"

    DEFAULT_INDEX_QUERY=("SELECT url, warc_filename, warc_record_offset, warc_record_length" +
        " FROM ccindex WHERE subset = 'warc' AND content_languages='eng' " +
        "AND (position('news' in url_host_name) != 0)")

    def parse_arguments(args, script_name):
        """ Returns the parsed arguments from the command line """

        arg_parser = argparse.ArgumentParser(prog=script_name,
            description="Copy common crawl index into our account and reparition",
            conflict_handler='resolve')
        arg_parser.add_argument("--crawl", type=str, required=True,
                                help='crawl')
        arg_parser.add_argument("--buckets", type=int, required=False, default=10,
                                help="Number of buckets")
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
        index_partition_path = "{}/{}".format(CC_INDEX_S3_PATH, crawl_partition_spec)

        df = spark.read.load(index_partition_path)
        df.createOrReplaceTempView("ccindex")

        sqldf = spark.sql(args.query)
        print("Executing query: {}".format(query))
        sqldf.explain()
        sqldf.show(10, False)
        sqldf.persist()
        num_rows = sqldf.count()
        print("Number of records/rows matched by query: {}".format(num_rows))
        sqldf = sqldf.repartition(args.buckets, "url")
        sqldf.write.mode("overwrite").parquet(MY_S3_CRAWL_INDEX_PATH)

if __name__ == '__main__':
    repartitionIndex = RepartitionCCIndex()
    repartitionIndex.run()
  
