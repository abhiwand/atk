#!/bin/bash
#copies all the hdfs data into s3. 
#Requirements:
#   Aws cli is needed to be pre configured for the script to work or the required access keys will have to passed to the script as command line options
#the script will stop,start habase has part of the copy process. If hbase is not shutdown nothing will get copied correctly
#all data is put in a preconfigured s3 bucket partitioned by email. thier is no need to further partition by cluster id since cluster user mapping is one to one.
#FYI:
#   two possible s3 formats are available when doing distcp s3n:// and s3://. s3n:// copies the files stored in s3 to hdfs which means MyFile.csv
#   will look like the original file when it's copied over to s3.
#   s3:// will copy the hdfs blocks to s3 while still usefull for back up purposes we can't browse the files and see what has been backed up.
#   if we use s3n the biggest draw back would be our max file size limitation of 5trb if any file in hdfs is larger than 5trb this will not work. 
#   Since s3:// copies hdfs blocks to s3 we are still bound by the max file size but no single block will should be any near the 5trb size which means we will be able to support larger file sizes. 

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

log "delete old logs directory"
runuser -l ec2-user -c "aws s3 rm s3://$BUCKET/$email/logs --recursive"

log "stop s3copier"
sudo initctl stop s3copier

log "stop hbase"
runuser -l hadoop -c "stop-hbase.sh"

log "zip .intelanalytics"
runuser -l hadoop -c " zip -r $IA_NOTEBOOKS .intelanalytics/* "I

log "remove any old $IA_NOTEBOOKS in hdfs"
runuser -l hadoop -c " hadoop fs -rm $IA_NOTEBOOKS"

log "put new $IA_NOTEBOOKS in hdfs"
runuser -l hadoop -c " hadoop fs -put $IA_NOTEBOOKS $IA_NOTEBOOKS "

log "delete $IA_NOTEBOOKS on local file system"
runuser -l hadoop -c " rm $IA_NOTEBOOKS"

log "run distcp from hdfs:/ to s3n://$BUCKET/$email/hdfs"
runuser -l hadoop -c "hadoop distcp2 -delete -update -log s3n://$access:$secret@$BUCKET/$email/logs hdfs:// s3n://$access:$secret@$BUCKET/$email/hdfs"

log "start s3copier"
sudo initctl start s3copier

log "start hbase"
runuser -l hadoop -c "start-hbase.sh" 

log "restart thrift"
runuser -l hadoop -c " hbase-daemon.sh start thrift -threadpool; sleep 2"

log "delete $IA_NOTEBOOKS in hdfs "
runuser -l hadoop -c " hadoop fs -rm $IA_NOTEBOOKS"
