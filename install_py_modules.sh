#!/bin/bash

sudo curl -O https://bootstrap.pypa.io/get-pip.py
sudo python3 get-pip.py --user
sudo /usr/bin/pip3 install botocore boto3 ujson warcio idna beautifulsoup4 lxml
