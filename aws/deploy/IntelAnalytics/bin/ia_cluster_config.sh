#!/bin/bash

RSYNC_ROOT=${HOME}/IntelAnalytics

# TODO: should have a globl file track all version and source into here
RSYNC_HDOOP=${RSYNC_ROOT}/hadoop/conf
RSYNC_HBASE=${RSYNC_ROOT}/hbase/conf

RSYNC_ZKEEP=${RSYNC_ROOT}/zookeeper/conf
RSYNC_TTNHB=${RSYNC_ROOT}/titan
RSYNC_PIG=${RSYNC_ROOT}/pig
RSYNC_MAHOUT=${RSYNC_ROOT}/mahout
RSYNC_GIRAPH=${RSYNC_ROOT}/girap

RSYNC_NODES=${RSYNC_HDOOP}/slaves

CMMD=$1
if [ -z "${CMMD}" ]
then
        exit -1
fi
for node in `cat ${RSYNC_NODES}`
do
        echo "## \"${CMMD}\":working on cluster node ${node}..."
        if [ "${CMMD}" == "--synconf" ]
        then
                if [ "${node}" = "`hostname -s`" ]
                then
                        echo "excluding self as ${node}..."
                        continue;
                fi
                rsync -avzl -e ssh ${RSYNC_HDOOP}/conf ${node}:${RSYNC_HDOOP}/conf
                rsync -avzl -e ssh ${RSYNC_HBASE}/conf ${node}:${RSYNC_HBASE}/conf
#               rsync -avzl -e ssh ${RSYNC_ZKEEP}/ ${node}:${RSYNC_ZKEEP}/
        elif [ "${CMMD}" == "--rmlogs" ]
        then
                ssh ${node} 'rm -rf ~/hadoop/logs/* ~/hbase/logs/*'
        elif [ "${CMMD}" == "--rmlogs-hadoop" ]
        then
                ssh ${node} 'rm -rf ~/hadoop/logs/*'
        elif [ "${CMMD}" == "--rmlogs-hbase" ]
        then
                ssh ${node} 'rm  -rf ~/hbase/logs/*'
        elif [ "${CMMD}" == "--rmlogs-zookeeper" ]
        then
                ssh ${node} 'rm ~/zookeeper/logs/*'
        elif [ "${CMMD}" == "--jpsls" ]
        then
                ssh ${node} 'jps -l | grep -v jps'
        fi
done

