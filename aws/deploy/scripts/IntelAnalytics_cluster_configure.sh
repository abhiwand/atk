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
    # make sure hostname is set up properly
    n1=`echo $n | awk -F'.' '{print $1}'`
    echo "Setting hostname for $n to $n1"
    ${dryrun} ssh -t -i ${pemfile} ${n} "sudo hostname ${n1}; sudo sed -i 's/.localdomain//g' /etc/sysconfig/network"
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

# get the master node ip
m=`sed '1q;d' ${nodesfile}`
# get the actual cluster size
csize=`cat ${nodesfile} | wc -l`

# update slave nodes ganglia config
${dryrun} scp -i ${pemfile} ${m}:/etc/ganglia/gmond.conf _gmond.master
for n in `cat ${nodesfile}`; do
    echo ${n}:Configuring ganglia...;
    if  [ "${n}" == "${m}" ]; then
        ${dryrun} ssh -t -i ${pemfile} ${n} sudo bash -c "'
        service gmond restart 2>&1 > /dev/null;
        service gmetad restart 2>&1 > /dev/null;
        service httpd stop 2>&1 > /dev/null;
        chkconfig httpd off 2>&1 > /dev/null;
        service nginx restart 2>&1 > /dev/null;
        '"
    else
        n0=`echo ${n} | awk -F '.' '{print $1}'`
        nn=`grep ${n0} ${hostsfile} | awk '{print $2}'`
        sed -e 's/host = \"127.0.0.1\"/host = \"master\"/g' -e 's/master@/'${nn}'@/g' _gmond.master > _gmond.${nn}
        ${dryrun} scp -i ${pemfile} _gmond.${nn} ${n}:/tmp/_gmond.conf
        ${dryrun} ssh -t -i ${pemfile} ${n} sudo bash -c "'
        echo ${n}:Updating ganglia config file;
        mv -f /tmp/_gmond.conf /etc/ganglia/gmond.conf;
        echo ${n}:Disabling webserver;
        service nginx stop 2>&1 > /dev/null;
        chkconfig nginx off 2>&1 > /dev/null;
        service httpd stop 2>&1 > /dev/null;
        chkconfig httpd off 2>&1 > /dev/null;
        service gmetad stop 2>&1 > /dev/null;
        chkconfig gmetad off 2>&1 > /dev/null;
        service gmond restart 2>&1 > /dev/null;
        '"
    fi
done
rm _gmond.* 2>&1 > /dev/null

# prepare to start the cluster/hadoop: nothing to do, default is configured
# with 4 nodes, so if that's the case, we don't have to do anything here as 
# the node AMI is already built w/ the correct hadoop/hbase configs based on
# 4-nodes master, node01, etc.
if [ ${csize} -ne 4 ]
then
    nodes="master"
    for ((i = 1; i < ${csize}; i++))
    do
        nodes="${nodes},`printf "%02d" ${i}`"
    done
    ${dryrun} ssh -i ${pemfile} ${IA_USR}@${m} bash -c "'
    echo $nodes | sed 's/,/\n/g' > hadoop/conf/slaves;
    echo $nodes | sed 's/,/\n/g' > hbase/conf/regionservers;
    sed -i \"s/storage.hostname=.*/"${nodes}"/g\" titan/conf/titan-hbase.properties;
    sed -i \"s/storage.hostname=.*/"${nodes}"/g\" titan/conf/titan-hbase-es.properties;
    sed -i \"s/<storage.hostname.*storage.hostname>/<storage.hostname>"${nodes}"</storage.hostname>/g\" titan/conf/rexstitan-hbase-es.xml;
    sed -i \"s/<server-host>.*/<server-host>0.0.0.0<\/server-host>/g\" titan/conf/rexstitan-hbase-es.xml;
    sed -i \"s/<base-uri>.*/<base-uri>http:\/\/localhost<\/base-uri>/g\" titan/conf/rexstitan-hbase-es.xml;
    '"
fi

# start hadoop/hbase, mount is handled by cloud.cfg now in cloud-init
${dryrun} ssh -i ${pemfile} ${IA_USR}@${m} bash -c "'
pushd ~/IntelAnalytics;
echo ${m}:Formatting hadoop name node on master node...;
hadoop/bin/hadoop namenode -format;
sleep 2;
echo ${m}:Starting hdfs...;
hadoop/bin/start-dfs.sh;
sleep 2;
echo ${m}:Starting mapred...;
hadoop/bin/start-mapred.sh;
sleep 2;
echo ${m}:Starting hbase...;
hbase/bin/start-hbase.sh;
sleep 2;
echo ${m}:Starting hbase thrift...;
hbase/bin/hbase-daemon.sh start thrift -threadpool;
sleep 2;
popd
'"
# Add more sanity check if needed, e.G., word-count, titan gods graph
${dryrun} ssh -i ${pemfile} ${IA_USR}@${m} bash -c "'
pushd ~/IntelAnalytics;
echo ${m}:Hadoop test using word count example...;
hadoop/bin/hadoop fs -mkdir wc;
sleep 1;
hadoop/bin/hadoop fs -put 4300.txt wc;
sleep 1;
hadoop/bin/hadoop jar hadoop/hadoop-examples-1.2.1.jar wordcount wc wc.out;
sleep 2;
echo ${m}:Creat Titan logging directory...;
mkdir -p /mnt/data1/logs/titan 2>&1 > /dev/null
ln -s /mnt/data1/logs/titan titan-server/logs 2>&1 > /dev/null
echo ${m}:Load Titan gods graph to hbase...;
titan/bin/gremlin.sh -e bin/IntelAnalytics_load.groovy;
echo ${m}:Start Titan/Rexster server...;
titan/bin/start-rexstitan.sh;
sleep 2;
popd
'"

if [ -f s3copier.jar ];
then
    echo "copy s3copier and start service"
    ${dryrun} sh IntelAnalytics_cluster_configure_s3copier.sh -p ${pemfile} -j s3copier.jar -c s3copier.conf -h ${m} -u ${IA_USR}
fi
