#!/bin/bash

sudo curl -O https://bootstrap.pypa.io/get-pip.py
sudo python3 get-pip.py --user
sudo /usr/bin/pip3 install botocore boto3 ujson warcio idna beautifulsoup4 lxml
sudo /usr/bin/pip3 install mysql-connector-python
sudo /usr/bin/pip3 install textblob
sudo /usr/bin/pip3 install nltk
sudo /usr/bin/pip3 install pandas
sudo /usr/bin/pip3 install dash==1.16.2
sudo python3 -c "import nltk; nltk.download('brown',download_dir='/usr/share/nltk_data');"
sudo python3 -c "import nltk; nltk.download('punkt',download_dir='/usr/share/nltk_data');"
sudo python3 -c "import nltk; nltk.download('wordnet',download_dir='/usr/share/nltk_data');"
sudo python3 -c "import nltk; nltk.download('averaged_perceptron_tagger',download_dir='/usr/share/nltk_data');"

# install airflow (from https://airflow.apache.org/docs/stable/start.html)
export AIRFLOW_HOME=~/airflow
sudo yum install python3-devel
sudo /usr/bin/pip3 install apache-airflow
airflow initdb

# start the web server, default port is 8081
# airflow webserver -p 8081
# start the scheduler
# airflow scheduler


# install for dash ec2 instance
# sudo curl -O https://bootstrap.pypa.io/get-pip.py
# sudo apt-get install python3-distutils
# sudo python3 get-pip.py
# sudo pip3 install mysql-connector-python
# sudo pip3 install pandas
# sudo pip3 install dash==1.16.2

# redirect port 80 to port 8050 to make port 80 work (see commoncrawl.org/the-data/get-started/)
# sudo iptables -t nat -I PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8050
