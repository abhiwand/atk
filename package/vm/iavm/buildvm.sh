#!/bin/bash

rm -rf output-virtualbox-ovf

packer build iavm.json

pushd output-virtualbox-ovf

	tar -pczf ../IntelAnalytics-0.8.0-CDH-5.0.3-internal-dev.tar.gz .

popd

rm /var/www/vm/IntelAnalytics-0.8.0-CDH-5.0.3-internal-dev.tar.gz

mv IntelAnalytics-0.8.0-CDH-5.0.3-internal-dev.tar.gz /var/www/vm/




rm -rf output-virtualbox-ovf

packer build iavmProd.json

pushd output-virtualbox-ovf

        tar -pczf ../IntelAnalytics-0.8.0-CDH-5.0.3.tar.gz .

popd

rm /var/www/vm/IntelAnalytics-0.8.0-CDH-5.0.3.tar.gz

mv IntelAnalytics-0.8.0-CDH-5.0.3.tar.gz /var/www/vm/

