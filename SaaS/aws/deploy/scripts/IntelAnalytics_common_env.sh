# Common global settings for IntelAnalytics AWS
# This used for both AMI preparation and cluster creation.
nodesFQDN=$()
# All customer clusters will be prefixed w/ this string
export IA_NAME=IntelAnalytics
export IA_VERSION=0.5

# Working directory for deployment: both AMI and Cluster, set this in your own env
# e.g., export IA_HOME=~/myproject
if [ -z "${IA_HOME}" ]; then
    export IA_HOME=${HOME}/${IA_NAME}/${IA_VERSION}
fi
if [ ! -d ${IA_HOME} ]; then
    echo "Could not find working directory \"${IA_HOME}\"!"
    exit -1
fi

# deploy stuffs are here
export IA_DEPLOY=${IA_HOME}/deploy
export IA_CONFIG=${IA_DEPLOY}/config
export IA_CLUSTERS=${IA_DEPLOY}/clusters
export IA_LOGFILE=${IA_DEPLOY}/${IA_NAME}-${IA_VERSION}.log
# credentials are here
if [ -z "${IA_CREDENTIALS}" ]; then
    export IA_CREDENTIALS=${IA_HOME}/credentials
fi
if [ ! -d ${IA_CREDENTIALS} ]; then
    echo "Assuming credentials are at \"${IA_CREDENTIALS}\""
fi

# Overwrite IA_TAG to create the cluster to a different VPC
# IA_TAG is the name tag for the target VPC, used in all ec
# commands to filter out resources
if [ -z "${IA_TAG}" ]; then
    export IA_TAG=${IA_NAME}
fi

# Overwrite IA_AWS_REGION to create the cluster to a different region
if [ -z "${IA_AWS_REGION}" ]; then
    export IA_AWS_REGION=us-west-2
fi
export IA_EC2_URL=https://ec2.${IA_AWS_REGION}.amazonaws.com

# Overwrite IA_EC2_USR to use a different IAM, must have admin right
if [ -z "${IA_EC2_USR}" ];then
    export IA_EC2_USR=${IA_NAME}_Adm
fi

# SSH needs pem
export IA_EC2_PERMISSIONS=${IA_CREDENTIALS}/${IA_EC2_USR}.pem
# AMI CLI needs csv
export IA_EC2_CREDENTIALS=${IA_CREDENTIALS}/${IA_EC2_USR}.csv

# look for credential file in IA_CREDENTIALS
function IA_get_aws_access_key()
{
    local usr=$1
    local type=$2
    local crd=${IA_CREDENTIALS}/${usr}.csv

    if [ -f ${crd} ] || [ -h ${crd} ]; then
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
