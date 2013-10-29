#
# Global settings for per user/customer cluster
#
# TODO: dynamtically generate the IA_HOSTS
#

export IA_HOME=${HOME}/deploy
export IA_USER=${HOME}/deploy
export IA_NAME=IntelAnalytics
export IA_PKG=${IA_HOME}/packages/tribeca
export IA_PEM=${IA_HOME}/gaoyi.pem
export IA_USR=hadoop
export IA_UID=5002
export IA_USRSSH=${IA_HOME}/sshconf

export IA_FILES=( \
"hadoop-1.2.1.tar.gz" \
"hbase-0.94.12-security.tar.gz" \
"titan-all-0.4.0.tar.gz" \
"titan-server-0.4.0.tar.gz" \
"pig-0.12.0.tar.gz" \
"Python-2.7.5.tgz")

# target package
export IA_PACKAGE=${IA_NAME}.tar.gz

# TODO: this should be an input
export IA_HOSTS=${IA_HOME}/customers/IntelAnalytics-0.hosts
