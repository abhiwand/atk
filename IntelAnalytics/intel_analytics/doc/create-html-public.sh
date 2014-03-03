# Make sure env vars are set: CLUSTER_HOSTS, HADOOP_HOME, TITAN_HOME
set ch = "bogus CLUSTER_HOSTS var set for doc creation"
set hh = "bogus HADOOP_HOME var set for doc creation"
set th = "bogus TITAN_HOME var set for doc creation"

if [ -z "$CLUSTER_HOSTS" ]; then
    export CLUSTER_HOSTS=ch
fi
if [ -z "$HADOOP_HOME" ]; then
    export HADOOP_HOME=hh
fi
if [ -z "$TITAN_HOME" ]; then
    export TITAN_HOME=th
fi

python intel_analytics/doc/efuncgen.py > intel_analytics/doc/source/efunc.rst
make -B html -C intel_analytics/doc
tar -zcvf intel_analytics_pydoc.tar.gz intel_analytics/doc/build

# undo if we made any changes above
if [ "$CLUSTER_HOSTS" == ch ]; then
    unset CLUSTER_HOSTS
fi
if [ "$HADOOP_HOME" == hh ]; then
    unset HADOOP_HOME
fi
if [ "$TITAN_HOME" == th ]; then
    unset TITAN_HOME
fi
