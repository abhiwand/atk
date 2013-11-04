#!/bin/bash
# Description: Used for preparing IntelAnalytics software on all nodes
# Note: Expected to be executed from admin node
source IntelAnalytics_setup_env.sh

for n in `cat ${IA_HOSTS}`
do
	f=${IA_PACKAGE}
    f1=$(basename ${f})
	echo Deploying ${f} to ${n}...
	scp -i ${IA_PEM} ${f} ${IA_USR}@${n}:~
	ssh -i ${IA_PEM} ${IA_USR}@${n} "tar -zxf ${f1}; rm -rf ${f1}"
done
