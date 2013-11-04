#
# Global settings for per user/customer cluster bring-up
#
# TODO: 
# - generate hosts the IA_HOSTS mapping to standard master, node${i},...to hosts file
# - seperate envs for deployment from cluster creation
# - everything is prefixed by IntelAnalytic
# - fix the credentials to allow multiple users to use this script
# - logging to file
# - settle down the requirement on max/min clustesr (customers) to be supported
# - settle down the requirement on max/min cluster nodes, default, is `seq 4,4,24`
# 
# Notes: this script needs AWS EC2 CLI, AMI CLI, IAM CLI
#
source IntelAnalytics_common_env.sh

# existing AMI Names (Gold Images)
export IA_AMI_MASTER="${IA_NAME}-Master"
export IA_AMI_SLAVE="${IA_NAME}-Slave"
export IA_AMI_ADMIN="${IA_NAME}-Admin"
export IA_AMI_WEBSRV="${IA_NAME}-WebSRV"
export IA_AMI_WEBRDS="${IA_NAME}-WebRDS"
# consolidate master and slave to use just one image
export IA_AMI_NODE="${IA_NAME}-Master"
# instance type is cc2.8xlarge
export IA_INSTANCE_TYPE=cc2.8xlarge

# Default IAM group and user
export IA_IAM_GROUP=${IA_NAME}_Public
export IA_IAM_USER=${IA_NAME}_User

# TODO: requirement on supported clusters
export IA_CLUSTER_ID_RSV=0
export IA_CLUSTER_ID_MIN=1
export IA_CLUSTER_ID_MAX=40
export IA_CLUSTER_SIZE_MIN=4
export IA_CLUSTER_SIZE_MAX=24
export IA_CLUSTER_SIZE_INC=4
# The pre-generated cluster CIDR file
export IA_CLUSTER_CIDR=${IA_NAME}_cidr.txt

# TODO: these are pre-existing shared resources, we can also 
# do a query at the beginning instead of hard-coding them
export IA_IP_ADMIN="10.0.60.92"
export IA_CIDR_ADMIN="${IA_IP_ADMIN}/32"
# L2 flat CIDR
export IA_CIDR_FULL=10.0.0.0/18
# reserved
export IA_CIDR_RESV=10.0.64.0/27
# start ip
export IA_IP_MIN=10.0.64.32

# ec2-describe-route-tables -F "vpc-id=vpc-c84049aa" | grep ROUTETABLE | awk '{print $2}'
export IA_ROUTETABLE="rtb-bd464fdf"
# every cluser has the following two security groups
export IA_SGROUP_HTTPS="sg-381c145a"
export IA_SGROUP_ADMSSH="sg-447e7526"
export IA_SGROUP_INTSSH="sg-1c70787e"
export IA_SGROUP_EXTSSH="sg-97c2caf5"

# Helpers

# validate the cluster id
function IA_check_cluster_id()
{
    IA_loginfo "Check cluster id $1..."

    if [ ! -z "${1}" ] &&  [ ${1} -ge ${IA_CLUSTER_ID_MIN} ] && [ ${1} -le ${IA_CLUSTER_ID_MAX} ]
    then
        return 0
    fi
    IA_logerr "Input cluster id ${1} is not in valid range of [${IA_CLUSTER_ID_MIN}, ${IA_CLUSTER_ID_MAX}]!"
    return 1
}

# validate the cluster size
# Supports 4, 8, 12, 16, 20 (5 bit netmask, but we max to 20 for cc2.8xlarge)
function IA_check_cluster_size()
{
    local size=$1
    local sizes=(`seq ${IA_CLUSTER_SIZE_MIN} ${IA_CLUSTER_SIZE_MIN} ${IA_CLUSTER_SIZE_MAX}`)
    local SIZES="`echo ${sizes[@]}`"

    IA_loginfo "Check cluster size ${size}..."
    for (( i = 0; i < ${#sizes[@]}; i++ ))
    do
        if [ "${size}" == "${sizes[$i]}" ]
        then
            return 0
        fi
    done
    IA_logerr "Input cluster size ${size} is not supported! Valid sizes are ${SIZES}"
    return 1
}

# validate the cluster CIDR, use the pre-generated cluster cidr file, each line is in the format
# <cluster-id>  <cluster-cidr>
# Returns 0 for success, 1 for failure
# Sets _RET to be the placement group name
function IA_check_cluster_cidr()
{
    local cid=$1
    local line=$((${cid}+1))
    local cidr=(`sed "${line}q;d" ${IA_CLUSTER_CIDR}`)

    if [ ${cidr[0]} -eq ${cid} ]; then
        _RET=${cidr[1]}
        return 0
    fi
    IA_logerr "Input cluster id ${cid} != cidr file cluster id ${cidr[0]}!"
    _RET=""
    return 1
}

# get cluster name
function IA_format_cluster_name()
{
    echo ${IA_NAME}-$1
}

function IA_format_node_name_role()
{
    local nid=$1

    if [ ${nid} -eq 0 ]; then
        echo master
    else
        echo "node`printf "%02d" ${nid}`"
    fi
}

function IA_format_node_name()
{
    local cname=$1
    local nid=$2

    echo ${cname}-`IA_format_node_name_role ${nid}`
}


#shorter format for hostname, using 'ia' for 'IntelAnalytics"
function IA_format_node_hostname()
{
    local cname=$1
    local nid=$2

    nname=${cname}-`IA_format_node_name_role ${nid}`
    echo ${nname} | sed 's/IntelAnalytics/ia/g'
}


# get ami id by name
function IA_get_ami()
{
    echo `ec2-describe-images ${IA_EC2_OPTS_TAG} -F "name=${1}" -o self | grep IMAGE | awk '{print $2}'`
}

# get instance type
function IA_get_instance_type()
{
    echo ${IA_INSTANCE_TYPE}
}

# get vpc id
function IA_get_vpc()
{
    echo `ec2-describe-vpcs ${IA_EC2_OPTS_TAG} | grep VPC | awk '{print $2}'`
}

# get iam group: use the predefined one
function IA_get_iamgroup()
{
    echo ${IA_IAM_GROUP}
}

# get iam user: use the predefined one
function IA_get_iamuser()
{
    echo ${IA_IAM_USER}
}

# get route table
function IA_get_routetable()
{
    echo ${IA_ROUTETABLE}
}

# get per vpc security group for external HTTPS
function IA_get_sgroup_https()
{
    echo ${IA_SGROUP_HTTPS}
}

# get per vpc security group for external Admin SSH
function IA_get_sgroup_admssh()
{
    echo ${IA_SGROUP_ADMSSH}
}

# find a given placement group by name
function IA_find_pgroup()
{
    # check if the group exsits
    echo `ec2-describe-placement-groups ${IA_EC2_OPTS} -F "group-name=${1}" | awk '{print $2}'`
}

# create per cluster unique placement group
# Returns 0 for success, 1 for failure
# Sets _RET to be the placement group name
function IA_create_pgroup()
{
    # check if the group exsits
    local pgrp_name=$1

    IA_loginfo "Preparing to create placement group ${pgrp_name}..."
    _RET=`IA_find_pgroup ${pgrp_name}`
    if [ ! -z "${_RET}" ] && [ "${pgrp_name}" == "${_RET}" ]; then
        IA_loginfo "Found existing placement group ${pgrp_name}!"
        return 0
    fi

    ec2-create-placement-group ${IA_EC2_OPTS} -s cluster ${pgrp_name}
    _RET=`IA_find_pgroup ${pgrp_name}`
    if [ ! -z "${_RET}" ] && [ "${pgrp_name}" == "${_RET}" ]; then
        IA_loginfo "Created placement group ${pgrp_name}..."
        return 0
    fi
    _RET=""
    IA_logerr "Failed to create placement group ${pgrp_name}..."
    return 1
}

# find a given security group by name and vpc, returs the gorup id
function IA_find_sgroup()
{
    echo `ec2-describe-group ${IA_EC2_OPTS} -F "group-name=${1}" -F "vpc-id=${2}" | grep GROUP | awk '{print $2}'`
}


# create per cluster unique security group, this security group contains 3 rules
# - inbound, ssh only from everyone in the cluster
# - inbound, ssh only from the admin node
# - outbound, all
#
# Returns 0 for success, 1 for failure
# Sets _RET the security group id
function IA_create_sgroup()
{
    # check if the group exsits
    local sgrp_name=$1-ssh
    local sgrp_vpc=$2
    local sgrp_cidr=$3

    IA_loginfo "Prepare to create security group ${sgrp_name}..."
    _RET=`IA_find_sgroup ${sgrp_name} ${sgrp_vpc}`
    if [ ! -z "${_RET}" ]; then
        IA_loginfo "Found existing security group ${sgrp_name}, ${_RET}"
    else
        IA_loginfo "Creating security group ${sgrp_name}..."
        ec2-create-group ${IA_EC2_OPTS} ${sgrp_name} -d "Allow SSH Access for ${sgrp_name} within Cluster" -c ${sgrp_vpc}
        _RET=`IA_find_sgroup ${sgrp_name} ${sgrp_vpc}`
        if [ -z "${_RET}" ]; then
            IA_logerr "Failed to create security group ${sgrp_name}..."
            _RET=""
            return 1
        fi
        IA_loginfo "Created security group ${sgrp_name}, ${_RET}"
    fi

    # note, only check by grepping cidr, to be really careful, get the
    # corresponding columns, and compare all idr, to, and from columns
    ec2-describe-group ${IA_EC2_OPTS} -H -F "vpc-id=${sgrp_vpc}" -F "group-id=${_RET}" | grep "${sgrp_cidr}" 2>&1 > /dev/null
    if [ $? -ne 0 ];then
        IA_loginfo "Creating inbound cluster SSH rule for ${sgrp_name}..."
        ec2-authorize ${IA_EC2_OPTS} ${_RET} --protocol tcp --port-range 22 --cidr "${sgrp_cidr}"
    else
        IA_loginfo "Existing rule on ${sgrp_cidr} for ${sgrp_name} found..."
    fi
    return 0
}

# find the subnet for the target cluster
function IA_find_subnet()
{
    echo `ec2-describe-subnets ${IA_EC2_OPTS} -F "tag:Name=${1}" -F "vpc-id=${2}" -F "cidr=${3}" | grep SUBNET | awk '{print $2}'`
}

# create subnet/cidr for the target cluster
# Returns 0 for success, 1 for failure
# Sets _RET the subnet id
function IA_create_subnet()
{
    # check if the group exsits
    local snet_name=$1
    local snet_vpc=$2
    local snet_cidr=$3

    IA_loginfo "Prepare to create subnet ${snet_name}..."
    _RET=`IA_find_subnet ${snet_name} ${snet_vpc} ${snet_cidr}`
    if [ ! -z "${_RET}" ]; then
        IA_loginfo "Found existing subnet ${snet_name} with id ${_RET} in vpc ${snet_vpc}"
        return 0
    fi

    # default zone
    IA_loginfo "Creating subnet ${snet_name}..."
    ec2-create-subnet ${IA_EC2_OPTS} --vpc ${snet_vpc} --cidr ${snet_cidr}
    _RET=`IA_find_subnet ${snet_name} ${snet_vpc} ${snet_cidr}`
    if [ ! -z "${_RET}" ]; then
        IA_loginfo "Found existing subnet ${snet_name} with id ${_RET} in vpc ${snet_vpc}"
        ec2-create-tags ${IA_EC2_OPTS} ${snet_id} --tag "Name=${snet_name}"
        return 0
    fi
    IA_logerr "Failed to create subnet ${sne_name} for VPC ${snet_vpc} using CIR ${snet_cidr}!"
    _RET=""
    return 1
}

# find a given placement group by name
function IA_find_routetable_subnet()
{
    echo `ec2-describe-route-tables ${IA_EC2_OPTS} ${1} -F "association.subnet-id=${2}" | grep ${2} | awk '{print $3}'`
}

# update the route table to associte it with the given subnet
# Returns 0 for success, 1 for failure
# Sets _RET the subnet id that is associated to the route table
function IA_update_routes()
{
     # check if the group exsits
    local rt_id=$1
    local rt_vpc=$2
    local rt_snet=$3
    local snet

    IA_loginfo "Updating route table ${rt_id} in vpc ${rt_vpc} for subnet ${rt_snet}..."
    _RET=`IA_find_routetable_subnet ${rt_id} ${rt_snet}`
    if [ ! -z "${_RET}" ] && [ "${_RET}" == "${rt_snet}" ]; then
        IA_loginfo "Found existing association in route table ${rt_id} for subnet ${_RET}"
        return 0
    fi
    ec2-associate-route-table ${IA_EC2_OPTS} ${rt_id} --subnet ${rt_snet}
    _RET=`IA_find_routetable_subnet ${rt_id} ${rt_snet}`
    if [ ! -z "${_RET}" ] && [ "${_RET}" == "${rt_snet}" ]; then
        IA_loginfo "Updated route table with association with subnet ${_RET}"
        return 0
    fi
    IA_logerr "Failed to associate subnet ${rt_snet} with routing table ${rt_id}!"
    _RET=""
    return 1
}

# find out the ec2 instance id by the name tag
function IA_get_instance_id()
{
    local name=$1
    local vpc=`IA_get_vpc`

    echo `ec2-describe-instances -F "vpc-id=${vpc}" -F "tag:Name=${name}" | grep INSTANCE | awk '{print $2}' `
    return 0
}

# get the value of a given column for an ec2 instance
# this is extracted from ec2-describe-instances, and matches the ec2 docs 
# supported filter fields
function IA_get_instance_column()
{
    local ins=(`ec2-describe-instances --show-empty-fields -F "tag:Name=${1}" | grep INSTANCE`)

    case $2 in
    vpc-id)
        echo ${ins[18]}
        ;;
    instance-id)
        echo ${ins[1]}
        ;;
    public-dns-name)
        echo ${ins[3]}
        ;;
    private-dns-name)
        echo ${ins[4]}
        ;;
    public-ip-address)
        echo ${ins[16]}
        ;;
    private-ip-address)
        echo ${ins[17]}
        ;;
    *)
        ;;
    esac
}

# find out the ec2 instance ip by the name tag
function IA_get_instance_private_ip()
{
    echo `IA_get_instance_column $1 private-ip-address`
}

function IA_get_instance_private_dns()
{
    echo `IA_get_instance_column $1 private-dns-name`
}

function IA_get_instance_public_ip()
{
    echo `IA_get_instance_column $1 public-ip-address`
}
function IA_get_instance_public_dns()
{
    echo `IA_get_instance_column $1 public-dns-name`
}

function IA_generate_hosts_file()
{
    local cname=$1
    local csize=$2
    local outdir=$3

    if [ ! -d ${outdir} ]; then
        IA_logerr "Output director \"${outdir}\" not found!"
        return 1
    fi

    headers=${outdir}/headers.hosts
    if [ ! -f ${headers}} ]; then
        IA_logerr "Hosts file header \"${headers}\" not found!"
        return 1
    fi

    cname=`IA_format_cluster_name "${cid}-${csize}"`
    outhosts=${outdir}/${cname}.hosts
    outnodes=${outdir}/${cname}.nodes
    rm -f ${outhosts} 2>&1 > /dev/null
    rm -f ${outnodes} 2>&1 > /dev/null
    
    cat ${headers} > ${outhosts}
    for (( i = 0; i < ${csize}; i++ ))
    do  
        hosts[$i]=`IA_format_node_name_role $i`
        nname[$i]=`IA_format_node_name ${cname} $i`
        ip[$i]=`IA_get_instance_private_ip ${nname[$i]}`
        dnsfull[$i]=`IA_get_instance_private_dns ${nname[$i]}`
        dns[$i]=`echo ${dnsfull[$i]} | awk -F"." '{print $1}'`
        echo "${ip[$i]} ${hosts[$i]} ${dns[$i]}" >> ${outhosts}
        echo "${ip} ${host} ${dns}" >> ${outhosts}
        echo "${dnsfull[$i]}" >> ${outnodes}
    done
    IA_loginfo "Generated hosts file ${outhosts}..."
    IA_loginfo "Generated nodes file ${outnodes}..."
}
