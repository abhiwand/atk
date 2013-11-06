#!/bin/bash
# Description: Used for preparing hosts file for the target cluster
source IntelAnalytics_cluster_env.sh

function usage()
{
    echo "usage: --cluster-id <cid> [--cluster-size <csize>]"
    exit 1
}

# check inputs
csize=4
while [ $# -gt 0 ]
do
    case "$1" in
    --cluster-id)
        cid=$2
        shift 2
        ;;
    --cluster-size)
        csize=$2
        shift 2
        ;;
    *)
        usage
        ;;
    esac
done

if [ -z "${cid}" ] || [ -z "${csize}" ]; then
    usage
fi
IA_generate_hosts_file ${cid} ${csize} ${IA_CLUSTERS}
