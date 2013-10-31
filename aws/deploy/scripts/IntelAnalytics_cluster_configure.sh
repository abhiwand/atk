#!/bin/bash
# Description: Used for preparing IntelAnalytics cluster to start operation
# Note: Expected to be executed from admin node
# - Update config files
# - Start hdfs
# - Start hbase
# - Start iPython??

source IntelAnalytics_setup_env.sh

IA_HOSTS=$1
if [ ! -f ${IA_HOSTS} ]; then
    echo $(basename $0) <nodes_list_file>
fi

# prepare hadoop/hbase config: we probably don't have to do anything
# here, the AMIs has hadoop/hbase configs based on master, node01, etc.

# TODO: prepare to start the cluster/hadoop

# TODO: prepare to start the ipython server
