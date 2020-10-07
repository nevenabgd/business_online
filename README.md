# Business Online
Revealing online presence metrics for businesses

## Introduction
Business online presence is key for the success. If you are a business owner, wouldn’t it be nice if you could have a place to see how is your business doing online, who is talking about your business, and what is the overall sentiment across news or social media.
Solution: An application which provides businesses quick and easy access to their online presence metrics. Specifically, businesses can see how often are they mentioned online over time, what is the overall sentiment, and also they can compare how are they doing against some of other competing businesses.

## Dataset
Used the Common Crawl data set which is stored on Amazon S3 ([Common Crawl](https://commoncrawl.org/)). Data is saved in WARC files. It stores information about all URLs it crawled, including dates when they were crawled and html content.

## Architecture
![Pipeline](pipeline1.png)

## Creating Amazon EMR cluster
* Create the EMR cluster
* Go with the advanced installation
* Check Hadoop, Hive, Spark, Zeppelin
* Enable ssh and pick your ec2 key
* Setup a custom bootstrap option: upload install_py_modules.py to an s3 bucket and set it up as a bootstrap script

## Run a Spark common crawl processing job
* Clone the project git repository
* Run spark_submit_common_crawl_copy.sh