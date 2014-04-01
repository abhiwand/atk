#!/bin/bash
#copies/runs a script to copy data from s3 to hdfs using hadoop distcp2
#Requirements
#   aws cli pre configured for s3ToHdfs.sh to work or they have to be passed in.

TEMP=`getopt -o e: --long email:,nodes-file:,hosts-file:,pem-file:,dry-run -n 'IntelAnalytics_copy_s3ToHdfs.sh' -- "$@"`

if [ $? != 0 ]; then echo "Terminating ..." >&2 ; exit 1; fi

eval set -- "$TEMP"

function usage()
{
    echo "usage: $1 --email <cluster-owners-email/other identifier> --nodes-file <nodes-list-file> --hosts-file <hosts-file> --pem-file <pem-file> [--dry-run]"
    exit 1
}
echo "$1"
# Check inputs
dryrun=""
s3DistcpBucket="gao-cluster-hdfs"
s3ToHdfs="s3ToHdfs.sh"
while true; do
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
    --dry-run)
        dryrun="echo "
        shift 1
        ;;
    --) shift ; break ;;
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


echo "copy script to cluster" 
${dryrun} scp -i ${pemfile} ${s3ToHdfs} ${m}:~/${s3ToHdfs}
${dryrun} ssh -t -i ${pemfile} ${m} sudo bash -c  "' sudo  sh /home/ec2-user/$s3ToHdfs --bucket \\\"$s3DistcpBucket\\\" --email \\\"$email\\\"  '"
