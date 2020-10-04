import sys
sys.path.append('../')
import argparse
import logging
import os
import re

from io import BytesIO
from tempfile import TemporaryFile

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

import boto3
import botocore

from consts import CC_INDEX_S3_PATH, MY_S3_CRAWL_DATA_PATH, MY_S3_CRAWL_INDEX_PATH

import random

from urllib.parse import urlparse

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType


class DownloadCCData(object):
    """ """

    name = "DownloadCCData"
    
    output_schema = StructType([
        StructField("url", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("date", StringType(), True),
        StructField("text", StringType(), True)])

    warc_parse_http_header = True

    records_processed = None
    warc_input_processed = None
    warc_input_failed = None
    records_parsing_failed = None
    records_non_html = None

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

    def init_accumulators(self, sc):
        self.records_processed = sc.accumulator(0)
        self.warc_input_processed = sc.accumulator(0)
        self.warc_input_failed = sc.accumulator(0)
        self.records_parsing_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)

    def run(self):
        self.args = self.parse_arguments()

        conf = SparkConf()
        conf.set("spark.default.parallelism", 100)
        sc = SparkContext(appName=self.name, conf=conf)
        sqlc = SQLContext(sparkContext=sc)

        self.init_accumulators(sc)

        self.run_job(sc, sqlc)

        sc.stop()
    
    def log_aggregators(self, sc):
        print(sc, self.warc_input_processed, 'WARC/WAT/WET input files processed = {}')
        print(sc, self.warc_input_failed, 'WARC/WAT/WET input files failed = {}')
        print(sc, self.records_processed, 'WARC/WAT/WET records processed = {}')
        print(sc, self.records_parsing_failed, 'records failed to parse = {}')
        print(sc, self.records_non_html, 'records not HTML = {}')

    @staticmethod
    def is_html(record):
        """Return true if (detected) MIME type of a record is HTML"""
        html_types = ['text/html', 'application/xhtml+xml']
        if (('WARC-Identified-Payload-Type' in record.rec_headers) and
            (record.rec_headers['WARC-Identified-Payload-Type'] in
             html_types)):
            return True
        for html_type in html_types:
            if html_type in record.content_type:
                return True
        return False

    def html_to_text(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(page,
                                                               is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            for script in soup(["script", "style"]):
                script.extract()
            return soup.get_text(" ", strip=True).lower()
        except:
            self.records_parsing_failed.add(1)
            return ""

    def process_record(self, record):
        uri = record.rec_headers.get_header('WARC-Target-URI')
        uri_parsed = urlparse(uri)
        domain = uri_parsed.netloc
        date = record.rec_headers.get_header('WARC-Date')[:10]
        page = record.content_stream().read()
        if not self.is_html(record):
            self.records_non_html.add(1)
            return
        text = self.html_to_text(page, record)
        yield uri, domain, date, text

    def fetch_process_warc_records(self, rows):
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)
        bucketname = "commoncrawl"
        no_parse = (not self.warc_parse_http_header)

        for row in rows:
            url = row[0]
            warc_path = row[1]
            offset = int(row[2])
            length = int(row[3])
            rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
            try:
                response = s3client.get_object(Bucket=bucketname,
                                               Key=warc_path,
                                               Range=rangereq)
            except botocore.client.ClientError as exception:
                print(
                    'Failed to download: {} ({}, offset: {}, length: {}) - {}'
                    .format(url, warc_path, offset, length, exception))
                self.warc_input_failed.add(1)
                continue
            record_stream = BytesIO(response["Body"].read())
            try:
                for record in ArchiveIterator(record_stream,
                                              no_record_parse=no_parse):
                    for res in self.process_record(record):
                        self.records_processed.add(1)
                        yield res
            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                print(
                    'Invalid WARC record: {} ({}, offset: {}, length: {}) - {}'
                    .format(url, warc_path, offset, length, exception))

    def load_dataframe(self, sc, crawl_partition_spec, bucket_partition_spec):
        spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

        index_input_path = "{}/{}/{}".format(MY_S3_CRAWL_INDEX_PATH, crawl_partition_spec, bucket_partition_spec)

        df = spark.read.load(index_input_path)
        df.createOrReplaceTempView("ccindex")
        sqldf = spark.sql("SELECT url, warc_filename, warc_record_offset, warc_record_length FROM ccindex").orderBy("warc_filename")
        sqldf.persist()

        num_rows = sqldf.count()
        print(
            "Number of records/rows matched by query: {}".format(num_rows))

        return sqldf

    def run_job(self, sc, sqlc):
        crawl_partition_spec = "crawl={}".format(self.args.crawl)
        bucket_partition_spec = "bucket={}".format(self.args.bucket)
        sqldf = self.load_dataframe(sc, crawl_partition_spec, bucket_partition_spec)

        warc_recs = sqldf.select("url", "warc_filename", "warc_record_offset",
                                 "warc_record_length").rdd

        print("RDD: number of partitions: {}".format(warc_recs.getNumPartitions()))

        output = warc_recs.mapPartitions(self.fetch_process_warc_records)

        output_path = "{}/{}/{}".format(MY_S3_CRAWL_DATA_PATH, crawl_partition_spec, bucket_partition_spec)
        sqlc.createDataFrame(output, schema=self.output_schema) \
            .write \
            .mode("overwrite") \
            .parquet(output_path)

            # .option("spark.task.cpus", 0.5)

        self.log_aggregators(sc)


if __name__ == '__main__':
    job = DownloadCCData()
    job.run()
