#!/bin/bash
# For VPC GAODev,vpc-3a99d652
export IA_TAG=GAODev
export IA_SUBNET="172.31.32.0/20"
export IA_IP_ADMIN="172.31.42.246"
export IA_CIDR_ADMIN="${IA_IP_ADMIN}/32"
export IA_ROUTETABLE="rtb-3399d65b"
# every cluser has the following two security groups
export IA_SGROUP_HTTPS="sg-8fbca9ed"
export IA_SGROUP_ADMSSH="sg-aebca9cc"

if [ -z "${IA_HOME}" ]; then
    export IA_HOME=${HOME}/intel/IntelAnalytics/0.5
fi
if [ -z "${IA_CREDENTIALS}" ]; then
    export IA_CREDENTIALS=${IA_HOME}/credentials
fi

op=$1
shift 1
case ${op} in
cluster_create | cluster_configure | genhosts | genhosts_for_all)
    ./IntelAnalytics_${op}.sh $@
    ;;
*)
    echo "Unknown op: ${op}!"
    exit 1
esac
