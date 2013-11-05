#!/bin/bash
# Description: Used for preparing data disks on the cluster nodes
# - Format data disks on each node, assume to be xvd{b,c,d,e}, which
# is fixed as this is how the AMI is prepared
# - Mount data disks
# - Chown data mount points
#
# Note: Expected to be executed from admin node
#
# Usage:
#    --nodes-file the nodes list file w/ node's private ip/dns
#    --pem-file the SSH pem file (prv key) that is part of the AMI
#               for the use to perform ssh login to the node
#    --dry-run this only copies the execution script to node but
#              do not do anything
source IntelAnalytics_setup_env.sh

function usage()
{
    echo "usage: --nodes-file <nodes-list-file> --pem-file <ssh-user-pem-file> [--dry-run]"
    exit 1
}

dryrun=""
# Check inputs
while [ $# -gt 0 ]
do
    case "$1" in
    --nodes-file)
        nodesfile=$2
        shift 2
        ;;
    --pem-file)
        pemfile=$2
        shift 2
        ;;
    --dry-run)
        dryrun=$1
        shift 1
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
if [ -z "${pemfile}" ] || [ ! -f ${pemfile} ]; then
    echo "Could not locate the pem file \"${pemfile}\"!"
    usage
fi

_script=IntelAnalytics_setup_disks_node.sh
for n in `cat ${nodesfile}`
do
    # format disks
    scp -i ${pemfile} ${_script} ${n}:/tmp/${_script}
    ssh -i ${pemfile} -t ${n} "sudo nohup /tmp/${_script} ${dryrun}";
done
