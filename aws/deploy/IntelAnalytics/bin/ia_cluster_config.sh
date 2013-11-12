#!/bin/bash

IA_ROOT=${HOME}/IntelAnalytics

# TODO: should have a globl file track all version and source into here
IA_HDOOP=${IA_ROOT}/hadoop
IA_HBASE=${IA_ROOT}/hbase
IA_ZKEEP=${IA_ROOT}/zookeeper
IA_TTNHB=${IA_ROOT}/titan
IA_PIG=${IA_ROOT}/pig
IA_MAHOUT=${IA_ROOT}/mahout
IA_GIRAPH=${IA_ROOT}/girap


IA_HDOOP_LOG=`grep "^export HADOOP_LOG_DIR" ${IA_HDOOP}/conf/hadoop-env.sh | awk -F"=" '{print $2}'`
if [ -z "${IA_HDOOP_LOG}" ]; then
	IA_HDOOP_LOG=${IA_HDOOP}/logs
fi
IA_HBASE_LOG=`grep "^export HBASE_LOG_DIR" ${IA_HBASE}/conf/hbase-env.sh | awk -F"=" '{print $2}'`
if [ -z "${IA_HBASE_LOG}" ]; then
	IA_HBASE_LOG=${IA_HBASE}/logs
fi

# nodes, use hadoo slaves opr hbase regionservers
IA_NODES=${IA_HDOOP}/conf/slaves
CMMD=$1
if [ -z "${CMMD}" ]
then
        exit -1
fi
for node in `cat ${IA_NODES}`
do
        echo "## \"${CMMD}\":working on cluster node ${node}..."
        if [ "${CMMD}" == "--synconf" ]
        then
                if [ "${node}" = "`hostname -s`" ]
                then
                        echo "excluding self as ${node}..."
                        continue;
                fi
                rsync -avzl -e ssh ${IA_HDOOP}/conf/ ${node}:${IA_HDOOP}/conf/
                rsync -avzl -e ssh ${IA_HBASE}/conf/ ${node}:${IA_HBASE}/conf/
        elif [ "${CMMD}" == "--rmlogs" ]
        then
                ssh ${node} "rm -rf ${IA_HDOOP_LOG}/* ${IA_HBASE_LOG}/*"
        elif [ "${CMMD}" == "--rmlogs-hadoop" ]
        then
                ssh ${node} "rm -rf ${IA_HDOOP_LOG}/*"
        elif [ "${CMMD}" == "--rmlogs-hbase" ]
        then
                ssh ${node} "rm  -rf ${IA_HBASE_LOG}/*"
        elif [ "${CMMD}" == "--rmlogs-zookeeper" ]
        then
                ssh ${node} "rm ${IA_ZKEEP}/logs/*"
        elif [ "${CMMD}" == "--jpsls" ]
        then
                ssh ${node} "jps -l | grep -v jps"
        fi
done

