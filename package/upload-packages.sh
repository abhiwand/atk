#!/bin/bash
BUILD_SERVER=build.graphtrial.infra-host.com
BUILD_SERVER_UPLOAD_DIR=~/package/tmp
BUILD_SERVER_PEM=/root/.ssh/IntelAnalytics-build.pem
BUILD_SERVER_DIR=/home/ec2-user/package/tmp

ssh -o StrictHostKeyChecking=no -o ProxyCommand="nc -x  proxy-socks.jf.intel.com:1080 %h %p" -i ${BUILD_SERVER_PEM}  ec2-user@${BUILD_SERVER} mkdir -p $BUILD_SERVER_DIR/$BUILD_NUMBER

for file in `find . -name *.rpm`
do
	echo $file
#	scp -o StrictHostKeyChecking=no -o ProxyCommand="nc -x  proxy-socks.jf.intel.com:1080 %h %p" -i ${BUILD_SERVER_PEM} $file ec2-user@${BUILD_SERVER}:$BUILD_SERVER_DIR/$BUILD_NUMBER
done

for file in `find . -name *.deb`
do
        echo $file
#        scp -o StrictHostKeyChecking=no -o ProxyCommand="nc -x  proxy-socks.jf.intel.com:1080 %h %p" -i $BUILD_SERVER_PEM $file ec2-user@$BUILD_SERVER:$BUILD_SERVER_DIR/$BUILD_NUMBER
done

#run command to create repo
ssh -o StrictHostKeyChecking=no -o ProxyCommand="nc -x  proxy-socks.jf.intel.com:1080 %h %p" -i ${BUILD_SERVER_PEM}  ec2-user@${BUILD_SERVER} $BUILD_SERVER_DIR/../update-repo.sh -b $BRANCH -t branch
