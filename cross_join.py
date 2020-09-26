import argparse
import mysql.connector

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


class CrossJoin(object):

    def parse_arguments(args, script_name):
        """ Returns the parsed arguments from the command line """

        arg_parser = argparse.ArgumentParser(prog=script_name,
                                         description="Script copying data from s3 results table to MySQL DB",
                                         conflict_handler='resolve')
        arg_parser.add_argument("--input_company_path", type=str, required=True,
                                                 help='Input company path')
        arg_parsed.add_argument("--input_data_path", type=str,required+True,
                                                 help='input data path')
        arg_parser.add_argument("--endpoint", type=str, required=True,
                                                help="MySQL endpoint")
        arg_parser.add_argument("--user", type=str, required=True,
                                                help="User name")
        arg_parser.add_argument("--password", type=str, required=True,
                                                help="Password")
        arg_parser.add_argument("--db", type=str, required=True,
                                                help="Database name")

        args = arg_parser.parse_args()
        return args
    
    def run(self):
        args = self.parse_arguments("write_to_db.py")
        
        conf = SparkConf()
        sc = SparkContext(appName=self.name, conf=conf)
        session = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
        
        company_bucket =  args.input_company_path #"s3a://dataeng-bucket/company_data_us"
        input_bucket = args.input_data_path  #'s3a://dataeng-bucket/crawlerdata/news_2020_16/bucket=0'
        #input_bucket = args.input_data
        df_company= spark.read.parquet(company_bucket)
        df_company.createOrReplaceTempView("companies")
        
        #input_bucket = args.input_data_path  #'s3a://dataeng-bucket/crawlerdata/news_2020_16/bucket=0'
        df1 = spark.read.parquet(input_bucket)
        df1.createOrReplaceTempView("data")
        
        output_path = args.endpoint
        sqlDF = spark.sql("SELECT c.name, COUNT(*) as mentions, date "
                "FROM companies as c,data where (position(lower(c.name) in lower(text)) != 0) \
                group by c.name, date order by date")
        sqlDF.coalesce(1)
            .write
            .option("header","true")
            .option("sep",",")
            .mode("overwrite")
            .csv(output_path)
        

if __name__ == '__main__':
    cross_join = CrosJoin()
    cross_join.run()
  
