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

# Logging helper
function IA_logger()
{
    echo "### ${IA_NAME} $1"
}

function IA_loginfo()
{
    IA_logger "LOG:${1}"
}

function IA_logerr()
{
    IA_logger "ERR:${1}"
}
