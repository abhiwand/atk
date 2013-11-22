#!/bin/bash
source  `dirname $0`/IntelAnalytics_env.sh

HADOOP_LOG=`grep "^export HADOOP_LOG_DIR" ${HADOOP_HOME}/conf/hadoop-env.sh | awk -F"=" '{print $2}'`
if [ -z "${HADOOP_LOG}" ]; then
	HADOOP_LOG=${HADOOP_HOME}/logs
fi
HBASE_LOG=`grep "^export HBASE_LOG_DIR" ${HBASE_HOME}/conf/hbase-env.sh | awk -F"=" '{print $2}'`
if [ -z "${HBASE_LOG}" ]; then
	HBASE_LOG=${HBASE_HOME}/logs
fi

# nodes, use hadoo slaves opr hbase regionservers
NODES=${HADOOP_HOME}/conf/slaves
CMMD=$1
if [ -z "${CMMD}" ]
then
	echo "Useage: $(basename $0) [--synconf|--jpsls|--rmlogs|--rmlogs-hadoop|--rmlogs-base]"
        exit -1
fi
for node in `cat ${NODES}`
do
        echo "## \"${CMMD}\":working on cluster node ${node}..."
        if [ "${CMMD}" == "--synconf" ]
        then
                if [ "${node}" = "`hostname -s`" ]
                then
                        echo "excluding self as ${node}..."
                        continue;
                fi
                rsync -avzl -e ssh ${HADOOP_HOME}/conf/ ${node}:${HADOOP_HOME}/conf/
                rsync -avzl -e ssh ${HBASE_HOME}/conf/ ${node}:${HBASE_HOME}/conf/
        elif [ "${CMMD}" == "--rmlogs" ]
        then
                ssh ${node} "rm -rf ${HADOOP_LOG}/* ${HBASE_LOG}/*"
        elif [ "${CMMD}" == "--rmlogs-hadoop" ]
        then
                ssh ${node} "rm -rf ${HADOOP_LOG}/*"
        elif [ "${CMMD}" == "--rmlogs-hbase" ]
        then
                ssh ${node} "rm  -rf ${HBASE_LOG}/*"
        elif [ "${CMMD}" == "--jpsls" ]
        then
                ssh ${node} "jps -l | grep -v jps"
        elif [ "${CMMD}" == "--start" ]
		# start dfs
		${HADOOP_HOME}/bin/start-dfs.sh
		# start mapred
		${HADOOP_HOME}/bin/start-mapred.sh
		# start hbase
		${HBASE_HOME}/bin/start-hbase.sh
		# start hbase thrift
		${HBASE_HOME}/bin/hbase-daemon.sh start thrift
		# start titan server
		${TITAN_SERVER_HOME}/bin/start-rexstitan.sh
        elif [ "${CMMD}" == "--stop" ]
		# stop titan server
		${TITAN_SERVER_HOME}/bin/stop-rexstitan.sh
		# stop hbase thrift
		${HBASE_HOME}/bin/hbase-daemon.sh stop thrift
		# stop hbase
		${HBASE_HOME}/bin/stop-hbase.sh
		# stop mapred
		${HADOOP_HOME}/bin/stop-mapred.sh
		# stop dfs
		${HADOOP_HOME}/bin/stop-dfs.sh
        fi
done

