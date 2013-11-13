#!/bin/bash
# Description: This is a help to generate a hosts file that has clusters' nodes
# included. The per cluster alias host name will be like "ia-5-4-master}
#
source IntelAnalytics_cluster_env.sh

indir=${IA_CLUSTERS}
outdir=${IA_CLUSTERS}

function usage()
{
    echo "Inputs error!"
    echo "Usage: $1 [--cluster-size <csize>] [--input-dir <indir>] [--output-dir <outdir>]"
    exit 1
}

# check inputs
csize=4
while [ $# -gt 0 ]
do
    case "$1" in
    --cluster-size)
        csize=$2
        shift 2
        ;;
    --output-dir)
        outdir=$2
        shift 2
        ;;
    --input-dir)
        indir=$2
        shift 2
        ;;
    *)
        usage $(basename $0)
        ;;
    esac
done


if [ -z "${csize}" ] || [ ! -d "${indir}" ]; then
    usage
fi

if [ ! -d "${outdir}" ]; then
    IA_loginfo "Creating output diretory \"${outdir}\"..."
    mkdir -p ${outdir} 
fi
# loop through for all clusters nodes in input dir
ohost=${outdir}/${IA_AMI_ADMIN}.hosts
if [ -f ${ohost} ]; then
    IA_loginfo "Existing file \"${ohost}\" moved to \"${ohost}.bak\""
    mv -f ${ohost} ${ohost}.bak 2>&1 > /dev/null
fi
IA_loginfo "Will look for input cluster hosts file at \"${indir}\"..."
for (( i = ${IA_CLUSTER_ID_MIN}; i <= ${IA_CLUSTER_ID_MAX}; i++ ))
do
    cname=`IA_format_cluster_name "${i}-${csize}"`
    fhost=${indir}/${cname}.hosts
#   IA_loginfo "Checking hosts file \"${fhost}\" for \"${cname}\""
    if [ ! -f "${fhost}" ]; then
#       IA_loginfo "Not found hosts file \"${fhost}\" for \"${cname}\""
        continue
    fi
    IA_loginfo "Adding hosts for \"${cname}\" to \"${ohost}\""
    grep -v localhost ${fhost} | sed -e "s/master/"${cname}-master"/g" -e "s/node/"${cname}-node"/g" >> ${ohost}
done
echo "Collected hosts file generated at \"${ohost}\"..."
