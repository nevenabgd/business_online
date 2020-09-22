from collections import Counter

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCIndexWarcSparkJob


class CommonCrawlExtractor(CCIndexWarcSparkJob):
    """ """

    name = "CommonCrawlExtractor"
    
    output_schema = StructType([
        StructField("url", StringType(), True),
        StructField("text", StringType(), True)])

    records_parsing_failed = None
    records_non_html = None
    
    def add_arguments(self, parser):
        super(CommonCrawlExtractor, self).add_arguments(parser)
        agroup = parser.add_mutually_exclusive_group(required=False)
        agroup.add_argument("--s3_output_path", default=None,
                            help="S3 output location")

    def init_accumulators(self, sc):
        super(CommonCrawlExtractor, self).init_accumulators(sc)

        self.records_parsing_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)

    def log_aggregators(self, sc):
        super(CommonCrawlExtractor, self).log_aggregators(sc)

        self.log_aggregator(sc, self.records_parsing_failed,
                            'records failed to parse = {}')
        self.log_aggregator(sc, self.records_non_html,
                            'records not HTML = {}')

    def html_to_text(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(page,
                                                               is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            for script in soup(["script", "style"]):
                script.extract()
            return soup.get_text(" ", strip=True)
        except:
            self.records_parsing_failed.add(1)
            return ""

    def process_record(self, record):
        uri = record.rec_headers.get_header('uri')
        if uri is None:
            uri = record.rec_headers.get_header('WARC-Target-URI')
        page = record.content_stream().read()
        if not self.is_html(record):
            self.records_non_html.add(1)
            return
        # text = self.html_to_text(page, record)
        text = "test"
        yield uri, text
            
    def run_job(self, sc, sqlc):
        sqldf = self.load_dataframe(sc, self.args.num_input_partitions)

        warc_recs = sqldf.select("url", "warc_filename", "warc_record_offset",
                                 "warc_record_length").rdd

        output = warc_recs.mapPartitions(self.fetch_process_warc_records)
        
        if self.args.s3_output_path:
            sqlc.createDataFrame(output, schema=self.output_schema) \
                .coalesce(self.args.num_output_partitions) \
                .write.parquet(self.args.s3_output_path)
        else:
            sqlc.createDataFrame(output, schema=self.output_schema) \
                .coalesce(self.args.num_output_partitions) \
                .write \
                .format(self.args.output_format) \
                .option("compression", self.args.output_compression) \
                .options(**self.get_output_options()) \
                .saveAsTable(self.args.output)

        self.log_aggregators(sc)


if __name__ == '__main__':
    job = CommonCrawlExtractor()
    job.run()
