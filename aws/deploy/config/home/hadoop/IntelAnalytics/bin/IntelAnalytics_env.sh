if [ -z "${JAVA_HOME}" ]; then
	export JAVA_HOME=/usr/lib/jvm/java
fi
if [ -z "${INTEL_ANALYTICS_HOME}" ]; then
	export INTEL_ANALYTICS_HOME=/usr/lib/IntelAnalytics
	export INTEL_ANALYTICS_PYTHON=${INTEL_ANALYTICS_HOME}/virtpy/lib/python2.7/site-packages/intel_analytics
	export PATH=${PATH}:${INTEL_ANALYTICS_HOME}/bin
fi
if [ -z "${INTEL_ANALYTICS_JAR}" ]; then
    export INTEL_ANALYTICS_JAR=${INTEL_ANALYTICS_HOME}/IntelAnalytics-application-0.5-SNAPSHOT-jar-with-dependencies.jar
fi

if [ -z "${INTEL_ANALYTICS_HADOOP_HOME}" ]; then
	export INTEL_ANALYTICS_HADOOP_HOME=${HOME}/IntelAnalytics
	export PATH=${PATH}:${INTEL_ANALYTICS_HADOOP_HOME}/bin
fi

if [ -z "${HADOOP_HOME}" ]; then
	export HADOOP_HOME=${INTEL_ANALYTICS_HADOOP_HOME}/hadoop
	export PATH=${PATH}:${HADOOP_HOME}/bin
fi

if [ -z "${HBASE_HOME}" ]; then
	export HBASE_HOME=${INTEL_ANALYTICS_HADOOP_HOME}/hbase
	export PATH=${PATH}:${HBASE_HOME}/bin
fi

if [ -z "${PIG_HOME}" ]; then
	export PIG_HOME=${INTEL_ANALYTICS_HADOOP_HOME}/pig
	export PATH=${PATH}:${PIG_HOME}/bin
fi

if [ -z "${TITAN_HOME}" ]; then
	export TITAN_HOME=${INTEL_ANALYTICS_HADOOP_HOME}/titan
fi
if [ -z "${REXTER_HOME}" ]; then
	export REXTER_HOME=${INTEL_ANALYTICS_HADOOP_HOME}/titan-server
fi

if [ -z "${TITAN_SERVER_HOME}" ]; then
	export TITAN_SERVER_HOME=${INTEL_ANALYTICS_HADOOP_HOME}/titan-server
	export PATH=${PATH}:${TIAN_SERVER_HOME}/bin
fi
if [ -z "${ES_HOME}" ]; then
    export ES_HOME=${INTEL_ANALYTICS_HADOOP_HOME}/elasticsearch
    export ES_USER=hadoop
    export ES_GROUP=hadoop
    export PATH=${PATH}:${ES_HOME}/bin
fi
