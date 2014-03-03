#!/bin/bash
#copies and runs a script to back up hdfs data to s3 for back up with the use of hadoop distcp2
#This script can be run stand alone and will also get picked up by the configure script
#Step 1
#   create cron directory under ec2-user home
#Step 2
#   copy hdfsToS3.sh file to the created dir
#step 3   
#   set up our cron job to run at midnight
#step 4
#   run the script if run is set

function usage()
{
    echo "usage: $1 --email <cluster-owners-email/other identifier> --nodes-file <nodes-list-file> --hosts-file <hosts-file> --pem-file <pem-file> [--dry-run]"
    exit 1
}

# Check inputs
dryrun=""

while [ $# -gt 0 ]
do
    case "$1" in
    --email)
        email=$2
        shift 2
        ;;
    --nodes-file)
        nodesfile=$2
        shift 2
        ;;
    --hosts-file)
        hostsfile=$2
        shift 2
        ;;
    --pem-file)
        pemfile=$2
        shift 2
        ;;
    --run)
        run=$2
        shift 2
        ;;
    --dry-run)
        dryrun="echo "
        shift 1
        ;;
    *)
        usage $(basename $0)
        ;;
    esac
done

if [ "${email}" == "" ]; then
    echo "no email specified"
    usage $(basename $0)
fi

if [ -z "${nodesfile}" ] || [ ! -f ${nodesfile} ]; then
    echo "Could not find the nodes list file \"${nodesfile}\"!"
    usage $(basename $0)
fi
if [ -z "${hostsfile}" ] || [ ! -f ${hostsfile} ]; then
    echo "Could not find the hosts list file \"${hostsfile}\"!"
    usage $(basename $0)
fi

if [ -z "${pemfile}" ] || [ ! -f ${pemfile} ]; then
    echo "Could not locate the pem file \"${pemfile}\"!"
    usage $(basename $0)
fi

m=`grep "master" ${hostsfile} | awk -F" " '{print $1}'`

s3DistcpScript="hdfsToS3.sh"
s3DistcpBucket="gao-cluster-hdfs"
# set up s3Distcp back up on the master
${dryrun} ssh -i ${pemfile} ${m} "mkdir -p cron"
echo "copy hdfs backup cron script"
${dryrun} scp -i ${pemfile} ${s3DistcpScript} ${m}:~/cron/${s3DistcpScript}
echo "setup hdfs back up cron job"
${dryrun} ssh -t -i ${pemfile} ${m} sudo bash -c  "' sudo    echo \"00 00     * * *     root /home/ec2-user/cron/$s3DistcpScript --bucket \\\"$s3DistcpBucket\\\" --email \\\"$email\\\" \" > /etc/cron.d/hdfsToS3'"

if [ "$run" == "true" ]; then
    echo "run hdfs back up"
     ssh -t -i ${pemfile} ${m} sudo bash -c  "'sudo sh /home/ec2-user/cron/$s3DistcpScript --bucket \\\"$s3DistcpBucket\\\" --email \\\"$email\\\" '"
fi
