import argparse

from consts import MY_S3_CROSS_JOINED_DATA_PATH, MY_S3_AGGREGATED_DATA_PATH

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


class CompanyMentions(object):

    name = "CompanyMentions"

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        arg_parser = argparse.ArgumentParser(prog=self.name,
            description="Calculate mentions for a company for an entire crawl",
            conflict_handler='resolve')
        arg_parser.add_argument("--crawl", type=str, required=True,
                                help='crawl')
        args = arg_parser.parse_args()
        return args

    def run(self):
        args = self.parse_arguments()        
        conf = SparkConf()
        sc = SparkContext(appName=self.name, conf=conf)
        spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
        
        crawl_partition_spec = "crawl={}".format(args.crawl)
        data_input_path = "{}/{}".format(MY_S3_CROSS_JOINED_DATA_PATH, crawl_partition_spec)

        print("Reading input from path: {}".format(data_input_path))

        df = spark.read.load(data_input_path)
        df.createOrReplaceTempView("data")

        output_path = "{}/{}".format(MY_S3_AGGREGATED_DATA_PATH, crawl_partition_spec)
        print("Result output path: {}".format(output_path))

        sqlDF = spark.sql( \
                "SELECT d.company_name, d.date, count(*) as mentions" \
                "FROM data d group by d.company_name, d.date")
        sqlDF.write \
            .mode("overwrite") \
            .parquet(output_path)

        sc.stop()

if __name__ == '__main__':
    mentions = CompanyMentions()
    mentions.run()
