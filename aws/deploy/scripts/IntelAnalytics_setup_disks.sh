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
    echo "Usage: $1 --nodes-file <nodes-list-file> --pem-file <ssh-user-pem-file> [--dry-run]"
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
        dryrun="echo "
        shift 1
        ;;
    *)
        usage $(basename $0)
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

# FIXME: not sure exactly why, but the nohup, even w/ stderr/stout redirected,
# which according to man should work properly, still does not sometime work
# reliablly to actually executing the script on the remote side.
#
# The only work around is to execute this multiple times till the mkfs is kicked
# out on every nodes, there is no impact on an existing mkfs going on on a given disk
_script=IntelAnalytics_setup_disks_node.sh
for n in `cat ${nodesfile}`
do
    ${dryrun} scp -i ${pemfile} ${_script} ${n}:/tmp/${_script}
    echo "Copied \"${_script}\" to ${n}..."
    sleep 2s
done
sleep 5s
for n in `cat ${nodesfile}`
do
    echo "Executing \"${_script}\" on ${n}..."
    ${dryrun} ssh -i ${pemfile} -t ${n} "sudo bash -c '( ( nohup /tmp/${_script} &> /dev/null ) & )'";
    sleep 2s
done
# show the running process
for n in `cat ${nodesfile}`
do
    ${dryrun} ssh -i ${pemfile} -t ${n} "hostname; ps aux | grep mkfs";
done
echo All disks prepared ready!
