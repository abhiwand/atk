Inside Intel

add the following line to your /etc/apt/source.list file

deb https://bda-internal-apt.s3-us-west-2.amazonaws.com/MY-PACKAGE dev main

Then

sudo apt-get update

apt-cache search intelanalytics

To install run

sudo apt-cache install intelanalytics-rest-server
sudo apt-cache install intelanalytics-python-rest-client

Outside Intel

First add the public apt depency repo so you can install the s3 apt plugin

add the following line to your /etc/apt/source.list file

deb https://bda-public-repo.s3-us-west-2.amazonaws.com/apt deps main

then run

sudo apt-get install apt-transport-s3

After installing the S3 apt plugin add this line to your /etc/apt/source.list

deb s3://AKIAJVL3X4RZQK2ZHEPA:[B9PMgRaL+IDHOIajyJcXd2y70b5gcjw5LcEdY4xi]@bda-internal-apt.s3-us-west-2.amazonaws.com/MY-PACKAGE dev main

Then

sudo apt-get update
apt-cache search intelanalytics

To install use

sudo apt-cache install intelanalytics-rest-server