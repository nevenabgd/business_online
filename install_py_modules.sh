#!/bin/bash

sudo curl -O https://bootstrap.pypa.io/get-pip.py
sudo python3 get-pip.py --user
sudo /usr/bin/pip3 install botocore boto3 ujson warcio idna beautifulsoup4 lxml
sudo /usr/bin/pip3 install mysql-connector-python
sudo /usr/bin/pip3 install textblob
sudo /usr/bin/pip3 install nltk
sudo python3 -c "import nltk; nltk.download('brown',download_dir='/usr/share/nltk_data');"
sudo python3 -c "import nltk; nltk.download('punkt',download_dir='/usr/share/nltk_data');"
sudo python3 -c "import nltk; nltk.download('wordnet',download_dir='/usr/share/nltk_data');"
sudo python3 -c "import nltk; nltk.download('averaged_perceptron_tagger',download_dir='/usr/share/nltk_data');"
sudo python3 -c "import nltk; nltk.download('conll2000',download_dir='/usr/share/nltk_data');"
sudo python3 -c "import nltk; nltk.download('movie_reviews',download_dir='/usr/share/nltk_data');"

# All nltk
# sudo python3 -c "import nltk; nltk.download('all-nltk',download_dir='/usr/share/nltk_data');"
