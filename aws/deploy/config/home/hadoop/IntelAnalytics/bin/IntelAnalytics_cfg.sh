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
    if [ "${CMMD}" == "--synconf" ]
    then
        echo "## \"${CMMD}\":syncing config files for node ${node}..."
        if [ "${node}" = "`hostname -s`" ]
        then
                echo "excluding self as ${node}..."
                continue;
        fi
        rsync -avzl -e ssh ${HADOOP_HOME}/conf/ ${node}:${HADOOP_HOME}/conf/
        rsync -avzl -e ssh ${HBASE_HOME}/conf/ ${node}:${HBASE_HOME}/conf/
    elif [ "${CMMD}" == "--rmlogs" ]
    then
        echo "## \"${CMMD}\":clean up logs for node ${node}..."
        ssh ${node} "rm -rf ${HADOOP_LOG}/* ${HBASE_LOG}/*"
    elif [ "${CMMD}" == "--rmlogs-hadoop" ]
    then
        echo "## \"${CMMD}\":clean up hadoop logs for node ${node}..."
        ssh ${node} "rm -rf ${HADOOP_LOG}/*"
    elif [ "${CMMD}" == "--rmlogs-hbase" ]
    then
        echo "## \"${CMMD}\":clean up hbase logs for node ${node}..."
        ssh ${node} "rm  -rf ${HBASE_LOG}/*"
    elif [ "${CMMD}" == "--jpsls" ]
    then
        echo "## \"${CMMD}\":show jvm process status for node ${node}..."
        ssh ${node} "jps -l | grep -v jps"
    fi
done
# master only commands
if [ "master" == "`grep \`hostname\` /etc/hosts | awk '{print $2}'`" ]
then
    if [ "${CMMD}" == "--start" ]
    then 
        echo "## \"${CMMD}\":Starting all services..."
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
        # 
    elif [ "${CMMD}" == "--stop" ]
    then
        echo "## \"${CMMD}\":Stopping all services..."
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
    elif [ "${CMMD}" == "--titan-load-gods" ]
    then
        ${TITAN_HOME}/bin/gremlin.sh `dirname $0`/IntelAnalytics_load.grem

    elif [ "${CMMD}" == "--titan-test-gods" ]
    then
        ${TITAN_HOME}/bin/gremlin.sh `dirname $0`/IntelAnalytics_test.grem
    fi
fi
