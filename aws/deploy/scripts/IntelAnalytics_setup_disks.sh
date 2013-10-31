#!/bin/bash
# Description: Used for preparing data disks on the cluster nodes
# Note: Expected to be executed from admin node
# - Format data disks
# - Mount data disks
# - Chown data mount points

source IntelAnalytics_setup_env.sh

IA_HOSTS=$1
if [ ! -f ${IA_HOSTS} ]; then
    echo $(basename $0) <nodes_list_file>
    exit 1
fi

_script=IntelAnalytics_setup_disks_node.sh
for n in `cat ${IA_HOSTS}`
do
    # format disks
    scp -i ${IA_PEM} ${_script} ${n}:/tmp/${_script}
    ssh -i ${IA_PEM} -t ${n} "sudo /tmp/${_script}; sudo rm /tmp/${_script}";
done
