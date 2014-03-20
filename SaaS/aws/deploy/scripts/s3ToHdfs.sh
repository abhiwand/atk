#!/bin/bash
#This script will copy data from an s3 bucket that is partitioned by email to HDFS on the cluster
#The data in S3 should will most likely be prevoiusly backed up data from some hdfs cluster
#
#Requirements
#   The master node needs to have the aws cli installed and configured for this pull data from s3
#       since the required access and secret keys get pulled from the aws cli config file under the ec2-user account
#   
#
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

function log()
{
    echo "##INFO##-$1"
}

IA_NOTEBOOKS="intelanalytics-notebooks.zip"

log "delete log directory"
runuser -l ec2-user -c " aws s3 rm s3://$BUCKET/$email/logs --recursive "

log "stop hbase"
runuser -l hadoop -c " stop-hbase.sh "

log "run distcp2 from s3n://$BUCKET/$email/hdfs to hdfs:/"
runuser -l hadoop -c " hadoop distcp2 -delete -update -log s3n://$access:$secret@$BUCKET/$email/logs s3n://$access:$secret@$BUCKET/$email/hdfs  hdfs:// "

log "distcp2 done start-hbase"
runuser -l hadoop -c " start-hbase.sh "

log "restart thrift "
runuser -l hadoop -c " hbase-daemon.sh start thrift -threadpool; sleep 2"

log "remove old notebooks zip"
runuser -l hadoop -c " rm $IA_NOTEBOOKS "

log "get new notebooks zip"
runuser -l hadoop -c " hadoop fs -get $IA_NOTEBOOKS . "

log "unzip notebooks"
runuser -l hadoop -c " unzip -o $IA_NOTEBOOKS "

log "remove notebooks zip"
runuser -l hadoop -c " rm $IA_NOTEBOOKS "
