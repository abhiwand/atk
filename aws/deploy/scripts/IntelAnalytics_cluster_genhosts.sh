#!/bin/bash
# Description: Used for preparing hosts file for the target cluster
source IntelAnalytics_cluster_env.sh

outdir=${IA_CLUSTERS}

function usage()
{
    echo "Inputs error!"
    echo "Usage: $1 --cluster-id <cid> [--cluster-size <csize>] [--output-dir <outdir>]"
    exit 1
}

# check inputs
csize=4
while [ $# -gt 0 ]
do
    case "$1" in
    --cluster-id)
        cid=$2
        shift 2
        ;;
    --cluster-size)
        csize=$2
        shift 2
        ;;
    --output-dir)
        outdir=$2
        shift 2
        ;;
    *)
        usage $(basename $0)
        ;;
    esac
done

if [ -z "${cid}" ] || [ -z "${csize}" ] || [ ! -d "${outdir}" ]; then
    usage
fi
IA_generate_hosts_file ${cid} ${csize} ${outdir}
