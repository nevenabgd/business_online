import argparse

from consts import MY_S3_CROSS_JOINED_DATA_PATH, MY_S3_SENTIMENT_DATA_PATH

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

from textblob import TextBlob


class CompanySentiment(object):

    name = "CompanySentiment"

    output_schema = StructType([
        StructField("company_name", StringType(), True),
        StructField("date", StringType(), True),
        StructField("sentiment", LongType(), True)])

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        arg_parser = argparse.ArgumentParser(prog=self.name,
            description="Calculate sentiment for a company for an entire crawl",
            conflict_handler='resolve')
        arg_parser.add_argument("--crawl", type=str, required=True,
                                help='crawl')
        args = arg_parser.parse_args()
        return args

    def compute_sentiment(self, rows):
        for row in rows:
            company_name = row[0]
            date = row[1]
            text = row[2]
            blob = TextBlob(text)
            sentiment = 0.0
            for sentence in blob.sentences:
                sentiment += sentence.sentiment.polarity

            sentiment_score = int(sentiment * 100)  # (-100, 100)
            yield company_name, date, sentiment_score

    def run(self):
        args = self.parse_arguments()        
        conf = SparkConf()
        sc = SparkContext(appName=self.name, conf=conf)
        spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
        
        crawl_partition_spec = "crawl={}".format(args.crawl)
        data_input_path = "{}/{}".format(MY_S3_CROSS_JOINED_DATA_PATH, crawl_partition_spec)

        df = spark.read.load(data_input_path)
        df.createOrReplaceTempView("data")

        text_records = df.select("company_name", "date", "text").rdd
        
        output = text_records.mapPartitions(self.compute_sentiment)

        output_path = "{}/{}".format(MY_S3_SENTIMENT_DATA_PATH, crawl_partition_spec)
        print("Result output path: {}".format(output_path))

        sqlc = SQLContext(sparkContext=sc)
        sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(20) \
            .write \
            .mode("overwrite") \
            .parquet(output_path)

        sc.stop()

if __name__ == '__main__':
    sentiment = CompanySentiment()
    sentiment.run()
