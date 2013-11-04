#
# Common global settings for IntelAnalytics AWS
#
# TODO: 
# - generate hosts the IA_HOSTS mapping to standard master, node${i},...to hosts file
# - seperate envs for deployment from cluster creation
# - everything is prefixed by IntelAnalytic
# - fix the credentials to allow multiple users to use this script
# - logging to file
# - settle down the requirement on max/min clustesr (customers) to be supported
# - settle down the requirement on max/min cluster nodes, default, is `seq 4,4,24`
# - log to file
# 
# Notes: this script needs AWS EC2 CLI, AMI CLI, IAM CLI
#

# All customer clusters will be prefixed w/ this string
export IA_NAME=IntelAnalytics
export IA_TAG=${IA_NAME}
export IA_VERSION=0.5

# Working directory for deployment: both AMI and Cluster
if [ -z "${IA_HOME}" ]; then
    export IA_HOME=${HOME}/${IA_NAME}/${IA_VERSION}
fi
if [ ! -d ${IA_HOME} ]; then
    echo "Working directory is set as \"${IA_HOME}\""
fi

# deploy stuffs are here
export IA_DEPLOY=${IA_HOME}/deploy
export IA_CONFIG=${IA_DEPLOY}/config
export IA_CLUSTERS=${IA_DEPLOY}/clusters
export IA_LOGFILE=${IA_DEPLOY}/${IA_NAME}-${IA_VERSION}.log

# credentials are here
export IA_CREDENTIALS=${IA_HOME}/credentials
# TODO: default aws ec2 user, add a --user option later
if [ -z "${IA_EC2_USR}" ];then
    export IA_EC2_USR=gaoyi
fi
# SSH needs pem
export IA_EC2_PERMISSIONS=${IA_CREDENTIALS}/${IA_EC2_USR}.pem
export IA_EC2_CREDENTIALS=${IA_CREDENTIALS}/${IA_EC2_USR}.csv

# look for credential file in IA_CREDENTIALS
function IA_get_aws_access_key()
{
    local usr=$1
    local type=$2
    local crd=${IA_CREDENTIALS}/${usr}.csv
    
    if [ -f ${crd} ]; then
            case ${type} in
                aws-access-name)
                    echo `sed '2q;d' ${crd} | awk -F ',' '{print $1}'`
                    return 0
                    ;;
                aws-access-key)
                    echo `sed '2q;d' ${crd} | awk -F ',' '{print $2}'`
                    return 0
                    ;;
                aws-secret-key)
                    echo `sed '2q;d' ${crd} | awk -F ',' '{print $3}'`
                    return 0
                    ;;
                *)
                    ;;
            esac
    fi
    return 1
}

# EC2 needs key id and key
export IA_AWS_ACCESS_KEY=`IA_get_aws_access_key ${IA_EC2_USR} aws-access-key`
export IA_AWS_SECRET_KEY=`IA_get_aws_access_key ${IA_EC2_USR} aws-secret-key`

if [ -z "$IA_AWS_ACCESS_KEY}" ] || [ -z "${IA_AWS_SECRET_KEY}" ]; then
    echo "No valid AWS access key found for \"${IA_EC2_USR}\"!"
    exit -1
fi
# TODO: only supporst the following region at this moment
export IA_AWS_REGION=us-west-2
export IA_EC2_URL=https://ec2.us-west-2.amazonaws.com

# Build the default access command line op0tions
export IA_EC2_OPTS=" -O ${IA_AWS_ACCESS_KEY} -W ${IA_AWS_SECRET_KEY} --region ${IA_AWS_REGION}"
export IA_EC2_OPTS_TAG="${IA_EC2_OPTS} -F \"tag:Name=${IA_TAG}\""

# For AWS EC2, set to java-openjdk
if [ -z "${JAVA_HOME}" ]; then
    export JAVA_HOME=/usr/lib/jvm/default-java
fi

# Logging helper
function IA_logger()
{
    echo "### ${IA_NAME} $1" | tee -a ${IA_LOGFILE}
}

function IA_loginfo()
{
    IA_logger "LOG:${1}"
}

function IA_logerr()
{
    IA_logger "ERR:${1}"
}

function IA_logfile()
{
    IA_LOGFILE=${1}
}


