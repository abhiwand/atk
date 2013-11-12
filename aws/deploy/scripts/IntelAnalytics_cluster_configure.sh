#!/bin/bash
# Description: Used for configuring the cluster, but currently the only
# thing it does is to copy the hosts file to all nodes. There is no
# hadoop and hbase configuraion required as every AMI is built w/ the
# configuration using hosts alias names, i.e., master, node01 etc
#
# You still need to manually do
# - Start hdfs, mapred
# - Start hbase
# - Start iPython
#
# Notes"
# - Only the server componenbts
# - Need integration test w/ the iPython
# - Need integration test w/ the web front
# - Need integration test w/ s3 copy to hdfs
#
# Usage:
#    --nodes-file the nodes list file w/ node's private ip/dns
#    --hosts-file /etc/hosts file generated for the clustrer
#    --pem-file the SSH pem file (prv key) that is part of the AMI
#               for the use to perform ssh login to the node
#    --dry-run echo the commands instead of actually running it

source IntelAnalytics_setup_env.sh
source IntelAnalytics_cluster_env.sh

function usage()
{
    echo "usage: $1 --nodes-file <nodes-list-file> --hosts-file <hosts-file> --pem-file <pem-file> [--dry-run]"
    exit 1
}

# Check inputs
dryrun=""
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
    usage $(basename $0)
fi
if [ -z "${hostsfile}" ] || [ ! -f ${hostsfile} ]; then
    echo "Could not find the hosts list file \"${hostsfile}\"!"
    usage $(basename $0)
fi

if [ -z "${pemfile}" ] || [ ! -f ${pemfile} ]; then
    echo "Could not locate the pem file \"${pemfile}\"!"
    usage $(basename $0)
fi

# Update cluster-wide hosts file
for n in `cat ${nodesfile}`; do
    # update the host file
    echo "Updating the hosts file on node ${n}..."
    ${dryrun} scp -i ${pemfile} ${hostsfile} ${n}:/tmp/_hosts
    ${dryrun} ssh -t -i ${pemfile} ${n} "sudo mv -f /tmp/_hosts /etc/hosts"
    # remove the existing .ssh/known_hosts file
    ${dryrun} ssh -t -i ${pemfile} ${n} "sudo rm -f /home/hadoop/.ssh/known_hosts"
done

# Check data disk mounts and ownershipt, default user is 'hadoop'
if [ -z "${IA_USR}" ]; then
    IA_USR=hadoop
fi
for n in `cat ${nodesfile}`; do
    # Mount is handled by cloud.cfg now in cloud-init
    echo "Checking data disks mounts on node ${n}..."
    ${dryrun} ssh -t -i ${pemfile} ${n} "mount | grep xvd | grep data;"
    # Remove existing mount: by cloud-init now
    # ${dryrun} ssh -t -i ${pemfile} ${n} "sudo bash -c 'mount /dev/xvdb /mnt/data1; mount /dev/xvdc /mnt/data2; mount /dev/xvdd /mnt/data3; mount /dev/xvde /mnt/data4;'"
    # Check/enforce ownership
    ${dryrun} ssh -t -i ${pemfile} ${n} "sudo chown -R ${IA_USR}.${IA_USR} /mnt/data*"
done

# prepare to start the cluster/hadoop: nothing to do, already configured
# we don't have to do anything here as the node AMI is already built w/
# the correct hadoop/hbase configs based on master, node01, etc.

# start hadoop/hbase
n=`sed '3q;d' ${nodesfile}`
echo "Formatting hadoop name node on master node ${n}..."
${dryrun} ssh -t -i ${pemfile} hadoop@${n} "~/IntelAnalytics/hadoop/bin/hadoop namenode -format"

echo "Start hadoop..."
${dryrun} ssh -t -i ${pemfile} hadoop@${n} "~/IntelAnalytics/hadoop/bin/start-all.sh"

echo "Start hbase..."
${dryrun} ssh -t -i ${pemfile} hadoop@${n} "~/IntelAnalytics/hbase/bin/start-hbase.sh"
${dryrun} ssh -t -i ${pemfile} hadoop@${n} "~/IntelAnalytics/hbase/bin/hbase-daemon.sh start thrift -threadpool"
