#!/bin/bash
# help wrapper of
# ec2-describe-instances -O <o> -W <w> --show-empty-fields --region <r> -F "name:Nam=<input-name>"
source IntelAnalytics_cluster_env.sh

name=$1
if [ -z "${name}" ]; then
    echo "Must have a valid name name as input!"
    echo "Usage: $(basename $0) <instance-name-name>"
    exit 1
fi
echo "Query key-name for instance w/ name name ${name}..."
keyname=`IA_get_instance_column ${name} key-name`
if [ -z "${keyname}" ]; then
    echo "No key name is found for the given instance name name \"${name}\"!"
    exit 1
fi
echo "Key Name=\"${keyname}\" for instance name name \"${name}\"!"


