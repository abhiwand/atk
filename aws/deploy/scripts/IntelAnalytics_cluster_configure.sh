#!/bin/bash
# Description: Used for preparing IntelAnalytics cluster to start operation
# Note: Expected to be executed from admin node
# - Update config files
# - Start hdfs
# - Start hbase
# - Start iPython??

source IntelAnalytics_setup_env.sh
source IntelAnalytics_cluster_env.sh

function usage()
{
    echo "usage: --nodes-file <nodes-list-file> --hosts-file <hosts-file> --key-name <key-name>"
    exit 1
}

# Check inputs
while [ $# -gt 0 ]
do
    case "$1" in
    --nodes-file)
        nodesfile=$2
        shift 2
        ;;
    --hosts-file)
        hostsfile=$2
        shift 2
        ;;
    --key-name)
        keyname=$2
        shift 2
        ;;
    *)
        usage
        ;;
    esac
done

if [ -z "${nodesfile}" ] || [ ! -f ${nodesfile} ]; then
    echo "Could not find the nodes list file \"${nodesfile}\"!"
    usage
fi
if [ -z "${hostsfile}" ] || [ ! -f ${hostsfile} ]; then
    echo "Could not find the hosts list file \"${hostsfile}\"!"
    usage
fi

# get key name to associate with the .pem file for SSH access
if [ -z "${keyname}" ]; then
    echo "Must provide the valid key name for tis cluster!"
    usage
fi
pem=${IA_CREDENTIALS}/${keyname}.pem
if [ ! -f "${pem}" ]; then
    echo "Could not locate the pem file \"${pem}\" for key name \"${keyname}\"!"
    usage
fi

# Update cluster-wide hosts file
for n in `cat ${nodesfile}`; do
    # get the pem
    scp -i ${pem} ${hostsfile} ${n}:/tmp/_hosts
    ssh -t -i ${pem} ${n} "sudo mv -f /tmp/_hosts /etc/hosts"
done

# prepare hadoop/hbase config: we probably don't have to do anything
# here, the AMIs has hadoop/hbase configs based on master, node01, etc.

# TODO: prepare to start the cluster/hadoop

# TODO: prepare to start the ipython server
