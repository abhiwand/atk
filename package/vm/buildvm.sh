#!/bin/bash
#needs to run inside the packer/iavm directory

#copy salt tree for prod
cp -Rv ../salt/salt/base ../salt/salt/baseProd
cp -v ../salt/salt/top.sls ../salt/salt/baseProd

#create internal vm

#delete old exported vm
rm -rf output-virtualbox-ovf
#run packer
packer build iavm.json
#tar the two exported files a small xml descriptor file and the machine image
pushd output-virtualbox-ovf

	tar -pczf ../IntelAnalytics-0.8.0-CDH-5.0.3-internal-dev.tar.gz .

popd
#delete old hosted file
rm /var/www/vm/IntelAnalytics-0.8.0-CDH-5.0.3-internal-dev.tar.gz
#move new vm
mv IntelAnalytics-0.8.0-CDH-5.0.3-internal-dev.tar.gz /var/www/vm/




rm -rf output-virtualbox-ovf

packer build iavmProd.json

pushd output-virtualbox-ovf

        tar -pczf ../IntelAnalytics-0.8.0-CDH-5.0.3.tar.gz .

popd

rm /var/www/vm/IntelAnalytics-0.8.0-CDH-5.0.3.tar.gz

mv IntelAnalytics-0.8.0-CDH-5.0.3.tar.gz /var/www/vm/

