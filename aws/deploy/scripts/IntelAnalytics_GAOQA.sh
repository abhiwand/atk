#!/bin/bash
# For VPC GAOQA,vpc-3a99d652
export IA_TAG=GAOQA
export IA_SUBNET="10.10.0.0/18"
export IA_IP_ADMIN="10.10.52.201"
export IA_CIDR_ADMIN="${IA_IP_ADMIN}/32"
export IA_ROUTETABLE="rtb-bfc8c1dd"
# every cluser has the following two security groups
export IA_SGROUP_HTTPS="sg-b8b9acda"
export IA_SGROUP_ADMSSH="sg-8db9acef"

if [ -z "${IA_HOME}" ]; then
    export IA_HOME=${HOME}/intel/IntelAnalytics/0.5
fi
if [ -z "${IA_CREDENTIALS}" ]; then
    export IA_CREDENTIALS=${IA_HOME}/credentials
fi

op=$1
shift 1
case ${op} in
create | configure | genhosts | genhosts_for_all)
    ./IntelAnalytics_cluster_${op}.sh $@
    ;;
*)
    echo "Unknown op: ${op}!"
    exit 1
esac
