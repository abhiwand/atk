#!/bin/bash
source IntelAnalytics_env.sh

for n in `cat ${IA_HOSTS}`
do
	f=${IA_HOME}/${IA_PACKAGE}
	echo Deploying ${f} to ${n}...
	scp -i ${IA_PEM} ${f} ${IA_USR}@${n}:~
	ssh -i ${IA_PEM} ${IA_USR}@${n} "tar -zxf ${IA_PACKAGE}; rm -rf ${IA_PACKAGE}"
done
