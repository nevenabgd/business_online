import argparse
import mysql.connector

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


class DBWriter(object):

    name = "DBWriter"
    
    INSERT_COMPANY_METRICS = ("INSERT INTO CompanyMetrics "
               "(Company_name, Mentions, Date) "
               "VALUES (%s, %s, %s)")

    def parse_arguments(args, script_name):
        """ Returns the parsed arguments from the command line """

        arg_parser = argparse.ArgumentParser(prog=script_name,
                                         description="Script copying data from s3 results table to MySQL DB",
                                         conflict_handler='resolve')
        arg_parser.add_argument("--input_path", type=str, required=True,
                                                 help='Input path')
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
    
    def copy_s3_data_to_mysql(self):
        args = self.parse_arguments("write_to_db.py")
        cnx = mysql.connector.connect(user=args.user, password=args.password,
                              host=args.endpoint,
                              database=args.db)
                              
        cursor = cnx.cursor()
        cursor.execute("show databases")
        for name in cursor:
            print("Result is {}".format(name))
            
            
        conf = SparkConf()
        sc = SparkContext(
            appName=self.name,
            conf=conf)
        session = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
        sqldf = session.read.format("csv").option("header", True) \
                .option("inferSchema", True).load(args.input_path)
         
        sqldf.show(10, False)
        
        num_rows = sqldf.count()
        print("Inserting {} rows into db {}".format(num_rows, args.db))

        for row in sqldf.rdd.collect():
            metrics = (
                row[0],
                row[1],
                row[2]
                )
            print("{}".format(metrics))
            cursor.execute(self.INSERT_COMPANY_METRICS, metrics)
            
        print("Insert completed successfully!");
        
        cnx.close()
        

if __name__ == '__main__':
    db_writer = DBWriter()
    db_writer.copy_s3_data_to_mysql()
  
