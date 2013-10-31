#
# Global settings for deploying Intel Anaylitics on to an existing AMI.
# Descriptions;
# This is fed to the deploy scripts used to configure a cluster of instances
# with the given IntelAnalytics software, including, netowrk config, hardoop
# config, hbase config, IntelAnalytics software config.
#
# Notes:
# 1. This operation is expected one per IntelAnayltics software release, when
# new node AMI image needs to be recreated/updated.
# 2. This script is expected to be executed on the admin node with a running
# cluster
# 3. Currently, the running cluster that is used as the basis for preparing
# AMI is RHEL 6.4 based and has 4 nodes.
# 
# Default tag/name
source IntelAnalytics_common_env.sh

# TODO: these are for deployment, should be moved out to avoid confusion
export IA_HOME=${HOME}/deploy
export IA_USR=hadoop
export IA_UID=5002
export IA_CONFIG=${IA_HOME}/config
export IA_USRSSH=${IA_HOME}/sshconf
export IA_FILES=( \
  "hadoop-1.2.1.tar.gz" \
  "hbase-0.94.12-security.tar.gz" \
  "titan-all-0.4.0.tar.gz" \
  "titan-server-0.4.0.tar.gz" \
  "pig-0.12.0.tar.gz" \
  "Python-2.7.5.tgz")

# hard-coded, mapping is guaranteed by the original AMI creation
export IA_DISKS=(xvdb xvdc xvdd xvde)
export IA_PACKAGE=${IA_NAME}.tar.gz
export IA_HOSTS=${IA_HOME}/clusters/IntelAnalytics-0.hosts
export IA_PEM=${IA_HOME}/credentials/IntelAnalytics_User.pem
