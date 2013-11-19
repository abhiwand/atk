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
    ${dryrun} ssh -t -i ${pemfile} ${n} "sudo rm -f ~/.ssh/known_hosts /home/hadoop/.ssh/known_hosts"
done

# Check data disk mounts and ownershipt, default user is 'hadoop'
if [ -z "${IA_USR}" ]; then
    IA_USR=hadoop
fi
for n in `cat ${nodesfile}`; do
    # Mount is handled by cloud.cfg now in cloud-init
    ${dryrun} ssh -t -i ${pemfile} ${n} bash -c "'
    echo ${n}:Checking data disks mounts...;
    mount | grep xvd | grep data;
    sudo chown -R ${IA_USR}.${IA_USR} /mnt/data*;
    echo ${n}:Syncing up system time via ntp...;
    sudo service ntpd stop 2>&1 > /dev/null;
    sudo ntpdate 0.rhel.pool.ntp.org;
    sudo service ntpd start;
    '"
done

# prepare to start the cluster/hadoop: nothing to do, default is configured
# with 4 nodes, so if that's the case, we don't have to do anything here as 
# the node AMI is already built w/ the correct hadoop/hbase configs based on
# 4-nodes master, node01, etc.

# get the master node ip
n=`sed '1q;d' ${nodesfile}`
# get the actual cluster size
csize=`cat ${nodesfile} | wc -l`
if [ ${csize} -gt 4 ]
then
    ${dryrun} ssh -i ${pemfile} hadoop@${n} bash -c "'
    for ((i = 4; i < ${csize}; i++))
    do
        printf "%02d" ${i} >> hadoop/conf/slaves;
        printf "%02d" ${i} >> hbase/conf/regionservers;
    done;
    for ((i = 4; i < ${csize}; i++))
    do
        if [ ! -z "${nodes}" ]; then
            nodes="${nodes},`printf "%02d" ${i}`";
        fi
    done;
    sed -i \'s/node03/nodes03,"${nodes}"/g\' titan/conf/titan-hbase.properties;
    sed -i \'s/node03/nodes03,"${nodes}"/g\' titan/conf/titan-hbase-es.properties;
    '"
fi

# start hadoop/hbase, mount is handled by cloud.cfg now in cloud-init
${dryrun} ssh -i ${pemfile} hadoop@${n} bash -c "'
pushd ~/IntelAnalytics;
echo ${n}:Formatting hadoop name node on master node...;
hadoop/bin/hadoop namenode -format;
echo ${n}:Starting hdfs...;
hadoop/bin/start-dfs.sh;
echo ${n}:Starting mapred...;
hadoop/bin/start-mapred.sh;
echo ${n}:Starting hbase...;
hbase/bin/start-hbase.sh;
echo ${n}:Starting hbase thrift...;
hbase/bin/hbase-daemon.sh start thrift -threadpool;
'"
# Add more sanity check if needed, e.g., word-count, titan gods graph