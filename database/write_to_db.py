import sys
sys.path.append('../')
import argparse
import mysql.connector

from consts import MY_S3_MENTIONS_DATA_PATH, MY_S3_SENTIMENT_DATA_PATH

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


class DBWriter(object):

    name = "DBWriter"

    CREATE_TABLE = ("""
        CREATE TABLE IF NOT EXISTS company_metrics
        (
            id int not null auto_increment primary key,
            crawl varchar(30) not null,
            company_name varchar(100) not null,
            date date not null,
            metric_name varchar(20) not null,
            metric_value int
        )
    """)
    
    INSERT_COMPANY_METRICS = ("INSERT INTO company_metrics "
               "(crawl, company_name, date, metric_name, metric_value) "
               "VALUES (%s, %s, %s, %s, %s)")

    DELETE_CRAWL = ("DELETE FROM company_metrics "
               "WHERE crawl = '{}'")

    # table: top_domains
    # crawl, company_name, domain, mentions (only for top 3 domains)


    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        arg_parser = argparse.ArgumentParser(prog="write_to_db",
                                         description="Script copying data from s3 results table to MySQL DB",
                                         conflict_handler='resolve')
        arg_parser.add_argument("--crawl", type=str, required=True,
                                help='crawl')
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
        args = self.parse_arguments()
        cnx = mysql.connector.connect(user=args.user, password=args.password,
                              host=args.endpoint,
                              database=args.db)
                              
        cursor = cnx.cursor()
        cursor.execute("show databases")
        for name in cursor:
            print("Result is {}".format(name))
        
        print("Creating company_metrics table if not exist")
        cursor.execute(self.CREATE_TABLE)
        cnx.commit()

        print("Clearing data from the previous insert for crawl={}".format(args.crawl))
        cursor.execute(self.DELETE_CRAWL.format(args.crawl))
        cnx.commit()

        crawl_partition_spec = "crawl={}".format(args.crawl)
        
        conf = SparkConf()
        sc = SparkContext(
            appName=self.name,
            conf=conf)
        session = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

        print("Inserting mentions data")
        data_input_path = "{}/{}".format(MY_S3_MENTIONS_DATA_PATH, crawl_partition_spec)
        print("Reading input from path: {}".format(data_input_path))
        sqldf = session.read.parquet(data_input_path)
        
        sqldf.show(10, False)
        
        num_rows = sqldf.count()
        print("Inserting {} rows into db {}".format(num_rows, args.db))

        for row in sqldf.rdd.collect():
            metrics = (
                args.crawl,
                row[0],
                row[1],
                "mentions",
                row[2]
                )
            print("{}".format(metrics))
            cursor.execute(self.INSERT_COMPANY_METRICS, metrics)

        cnx.commit()

        print("Inserting sentiment data")
        data_input_path = "{}/{}".format(MY_S3_SENTIMENT_DATA_PATH, crawl_partition_spec)
        print("Reading input from path: {}".format(data_input_path))
        sqldf = session.read.parquet(data_input_path)
        
        sqldf.show(10, False)
        
        num_rows = sqldf.count()
        print("Inserting {} rows into db {}".format(num_rows, args.db))

        for row in sqldf.rdd.collect():
            metrics = (
                args.crawl,
                row[0],
                row[1],
                "sentiment",
                row[2]
                )
            print("{}".format(metrics))
            cursor.execute(self.INSERT_COMPANY_METRICS, metrics)
        
        cnx.commit()
        cursor.close()
        print("Insert completed successfully!");
        
        # Make sure data is committed to the database
        cnx.commit()
        cursor.close()
        cnx.close()
        

if __name__ == '__main__':
    db_writer = DBWriter()
    db_writer.copy_s3_data_to_mysql()
  
