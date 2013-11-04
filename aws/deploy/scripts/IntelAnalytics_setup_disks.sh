#!/bin/bash
# Description: Used for preparing data disks on the cluster nodes
# Note: Expected to be executed from admin node
# - Format data disks
# - Mount data disks
# - Chown data mount points

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
    echo "Could not locate the pem file \"${pem}\" for key name \"${keyname}\"!"
    usage
fi

_script=IntelAnalytics_setup_disks_node.sh
for n in `cat ${nodesfile}`
do
    # format disks
    scp -i ${pemfile} ${_script} ${n}:/tmp/${_script}
    ssh -i ${pemfile} -t ${n} "sudo nohup /tmp/${_script} ${dryrun}";
done
