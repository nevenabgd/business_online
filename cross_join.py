import argparse

from consts import MY_S3_CRAWL_DATA_PATH, MY_S3_COMPANY_DATA_PATH, MY_S3_CROSS_JOINED_DATA_PATH

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


class CrossJoin(object):

    name = "CrossJoin"

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        arg_parser = argparse.ArgumentParser(prog=self.name,
            description="Copy common crawl index into our account and reparition",
            conflict_handler='resolve')
        arg_parser.add_argument("--crawl", type=str, required=True,
                                help='crawl')
        arg_parser.add_argument("--bucket", type=str, required=True,
                                help="Url bucket to process")

        args = arg_parser.parse_args()
        return args
    
    def run(self):
        args = self.parse_arguments()
        
        conf = SparkConf()
        sc = SparkContext(appName=self.name, conf=conf)
        spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
        
        company_input_path =  MY_S3_COMPANY_DATA_PATH

        crawl_partition_spec = "crawl={}".format(args.crawl)
        bucket_partition_spec = "bucket={}".format(args.bucket)
        crawl_input_path = "{}/{}/{}".format(MY_S3_CRAWL_DATA_PATH, crawl_partition_spec, bucket_partition_spec)

        print("Reading companies input from: {}".format(company_input_path))
        df_company= spark.read.option("header", True).csv(company_input_path)
        df_company.createOrReplaceTempView("companies")
        df_company.show()
        
        print("Reading crawl input from: {}".format(crawl_input_path))
        df1 = spark.read.parquet(crawl_input_path)
        df1.show()
        df1.createOrReplaceTempView("data")

        output_path = "{}/{}/{}".format(MY_S3_CROSS_JOINED_DATA_PATH, crawl_partition_spec, bucket_partition_spec)

        print("Writing result to: {}".format(output_path))
        
        sqlDF = spark.sql( \
                "SELECT d.url, d.domain, d.text, d.date, c.name as company_name " \
                "FROM companies c, data d where (position(lower(c.name) in text) != 0)")
        sqlDF.write \
            .mode("overwrite") \
            .parquet(output_path)

        sc.stop()


if __name__ == '__main__':
    cross_join = CrossJoin()
    cross_join.run()
  
