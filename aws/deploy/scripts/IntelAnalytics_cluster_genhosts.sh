#!/bin/bash
# Description: Used for preparing hosts file for the target cluster
# Note: Expected to be executed from admin node
# - Format data disks
# - Mount data disks
# - Chown data mount points
# - Update hosts
# - Update config files 
# - Start hdfs
# - Start hbase
# - Start iPython??
#
# TODO: this is hard-coded right now, but should come from input file

source IntelAnalytics_cluster_env.sh

cid=$1
csize=$2
cname=`IA_format_cluster_name "${cid}-${csize}"`
outhosts=../clusters/${cname}.hosts
outnodes=../clusters/${cname}.nodes
headers=../clusters/headers.hosts

rm -f ${outhosts} 2>&1 > /dev/null
rm -f ${outnodes} 2>&1 > /dev/null

cat ${headers} > ${outhosts}
for (( i = 0; i < ${csize}; i++ ))
do  
    nname=`IA_format_node_name ${cname} $i`
    host=`IA_format_node_name_role $i`
    ip=`IA_get_instance_private_ip ${nname}`
    dnsfull=`IA_get_instance_private_dns ${nname}`
    dns=`echo ${dnsfull} | awk -F"." '{print $1}'`
    echo "${ip} ${host} ${dns}" >> ${outhosts}
    echo "${dnsfull}" >> ${outnodes}
done
