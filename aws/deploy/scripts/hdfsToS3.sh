#!/bin/bash

TEMP=`getopt -o a:s: --long access:,secret:,bucket:,email: -n 'IntelAnalytics_cluster_backup.sh' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

eval set -- "$TEMP"

access=$(cat /home/ec2-user/.aws/config | grep aws_access_key_id | awk -F" " '{print $3}')
secret=$(cat /home/ec2-user/.aws/config | grep aws_secret_access_key | awk -F" " '{print $3}')
BUCKET="gao-cluster-hdfs"
while true; do
    case "$1" in
        -a|--access)
            echo "Option a/access, argument '$2'"
            access=$2
            shift 2 ;;
        -s|--secret)
            echo "Option s/secret, argument '$2'"
            secret=$2
            shift 2 ;;
        --bucket)
            echo "Option bucket, argument '$2'"
            BUCKET=$2
            shift 2 ;;
        --email)
            echo "Option email, argument '$2'"
            email=$2
            shift 2 ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

runuser -l ec2-user -c "aws s3 rm s3://$BUCKET/$email/logs --recursive"

runuser -l hadoop -c "stop-hbase.sh"

runuser -l hadoop -c "hadoop distcp2 -delete -update -log s3n://$access:$secret@$BUCKET/$email/logs hdfs:// s3n://$access:$secret@$BUCKET/$email/hdfs"

runuser -l hadoop -c "start-hbase.sh" 

runuser -l hadoop -c " hbase-daemon.sh start thrift -threadpool; sleep 2"
