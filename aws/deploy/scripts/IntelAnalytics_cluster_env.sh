# Global settings for per user/customer cluster bring-up
# This has the cluster creation env settings and many aws cli wrapper functions
#
# Notes: this script needs AWS EC2 CLI, AMI CLI, IAM CLI
#
source IntelAnalytics_common_env.sh

# existing AMI Names (with build version, can override 
if [ -z "${IA_AMI_BUILD}" ]; then
    IA_AMI_BUILD="Build.14"
fi
export IA_AMI_VERSION="${IA_VERSION}-${IA_AMI_BUILD}"
export IA_AMI_MASTER="${IA_NAME}-Master-${IA_AMI_VERSION}"
export IA_AMI_SLAVE="${IA_AMI_MASTER}"
export IA_AMI_ADMIN="${IA_NAME}-Admin"
export IA_AMI_WEBSRV="${IA_NAME}-WebSRV"
export IA_AMI_WEBRDS="${IA_NAME}-WebRDS"
# consolidate master and slave to use just one image
export IA_AMI_NODE="${IA_AMI_MASTER}"
# instance type is cc2.8xlarge
export IA_INSTANCE_TYPE=cc2.8xlarge

# Default IAM group and user
export IA_IAM_GROUP=${IA_NAME}_Public
if [ -z "${IA_IAM_USER}" ]; then
    export IA_IAM_USER=${IA_NAME}_User
fi

# The current requirements on supported clusters are
# - max 40 clusters
# - default 4 nodes per cluster
# - max to 20 nodes per cluster
export IA_CLUSTER_ID_RSV=0
export IA_CLUSTER_ID_MIN=1
export IA_CLUSTER_ID_MAX=40
export IA_CLUSTER_SIZE_MIN=4
export IA_CLUSTER_SIZE_MAX=20
export IA_CLUSTER_SIZE_INC=4

# These are pre-existing shared resources, we can also 
if [ -z "${IA_SUBNET}" ]; then
    export IA_SUBNET="10.0.0.0/18"
fi
if [ -z "${IA_IP_ADMIN}" ]; then
    export IA_IP_ADMIN="10.0.60.92"
fi
export IA_CIDR_ADMIN="${IA_IP_ADMIN}/32"
if [ -z "${IA_ROUTETABLE}" ]; then
    export IA_ROUTETABLE="rtb-bd464fdf"
fi
# every cluser has the following two security groups
if [ -z "${IA_SGROUP_HTTPS}" ]; then
    export IA_SGROUP_HTTPS="sg-381c145a"
fi
if [ -z "${IA_SGROUP_ADMSSH}" ]; then
    export IA_SGROUP_ADMSSH="sg-447e7526"
fi
# default port range for hadoop
if [ -z "${IA_SGROUP_HADOOP_PORT_MIN}" ]; then
    export IA_SGROUP_HADOOP_PORT_MIN=0
fi
if [ -z "${IA_SGROUP_HADOOP_PORT_MAX}" ]; then
    export IA_SGROUP_HADOOP_PORT_MAX=65535
fi
if [ -z "${IA_SGROUP_SSH_PORT}" ]; then
    export IA_SGROUP_SSH_PORT=22
fi
if [ -z "${IA_SGROUP_HTTPS_PORT}" ]; then
    export IA_SGROUP_HTTPS_PORT=443
fi
# The pre-generated cluster CIDR file
if [ -z "${IA_CLUSTER_CIDR}" ]; then
    export IA_CLUSTER_CIDR=${IA_TAG}_cidr.txt
fi
if [ ! -f "${IA_CLUSTER_CIDR}" ]; then
    echo "Did not find cluster \"${IA_TAG}\" CIDR list file \"${IA_CLUSTER_CIDR}\"!!"
    exit 1
fi

# sunet status
export IA_AWS_PENDING="pending"
export IA_AWS_AVAILABLE="available"

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
    echo ${IA_TAG}-$1
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
    echo `ec2-describe-images ${IA_EC2_OPTS} -F "name=${1}" -o self | grep IMAGE | awk '{print $2}'`
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
function IA_find_pgroup_state()
{
    # check if the group exsits
    echo `ec2-describe-placement-groups ${IA_EC2_OPTS} -F "group-name=${1}" | awk '{print $4}'`
}

#
# Return in format TAG <type> <id> <tag-name> <tag-value>
# e.g. TAG     instance        i-cb0994ff      test    testtag
function IA_add_name_tag()
{
	local id=$1
	local val=$2
    local result=(`ec2-create-tags ${IA_EC2_OPTS} ${id} --tag "Name=${val}"`)

    if [ ${#result[@]} -ne 5 ]; then
        IA_logerr "Failed to create name tag for id ${id} using value ${val}!"
        return 1
    fi

    if [ "${result[4]}" != "${val}" ]; then
        IA_logerr "Error in tag adding, expect ${val}, got ${result[4]}"
        return 1
    fi
    IA_loginfo "Successfully added name tag ${val} for id ${id}"
    return 0
}

# create per cluster unique placement group
# Returns 0 for success, 1 for failure
# Sets _RET to be the placement group name
function IA_create_pgroup()
{
    # check if the group exsits
    local pgrp_name=$1
    local pgrp_state

    IA_loginfo "Preparing to create placement group ${pgrp_name}..."
    _RET=`IA_find_pgroup ${pgrp_name}`
    if [ ! -z "${_RET}" ] && [ "${pgrp_name}" == "${_RET}" ]; then
        IA_loginfo "Found existing placement group ${pgrp_name}!"
        return 0
    fi

    _RET=`ec2-create-placement-group ${IA_EC2_OPTS} -s cluster ${pgrp_name} | awk '{print $2}'`
    if [ -z "${_RET}" ] || [ "${pgrp_name}" != "${_RET}" ]; then
        IA_logerr "Failed to create placement group ${pgrp_name}..."
        _RET=""
        return 1
    fi

    # check group status
    IA_loginfo "Created placement group ${pgrp_name}..."
    for (( i = 0; i < 5; i++ ))
    do
        IA_loginfo "Checking placement group ${_RET} state, count ${i}..."
        pgrp_state=`IA_find_pgroup_state ${_RET}`
        if [ "${pgrp_state}" == "${IA_AWS_AVAILABLE}" ]; then
            return 0
        fi
        sleep 5s
    done

    IA_logerr "Placement group ${pgrp_name} created, but in a wrong state \"${pgrp_state}\"..."
    _RET=""
    return 1
}

# find a given security group by name and vpc, returs the gorup id
function IA_find_sgroup()
{
    echo `ec2-describe-group ${IA_EC2_OPTS} -F "group-name=${1}" -F "vpc-id=${2}" | grep GROUP | awk '{print $2}'`
}

# find an ingress rule by port range
# return 0 for sucess
# no overwriting _RET
function IA_find_sgroup_ingress_rule()
{
    local vpc=$1
    local sgrp=$2
    local pmin=$3
    local pmax=$4
    local rules=(`ec2-describe-group ${IA_EC2_OPTS} --show-empty-fields -F "vpc-id=${1}" -F "group-id=${2}" | grep PERMISSION | grep ingress`)

    if [ -z "$4" ]; then
        pmax=${pmin}
    fi
    # 11 fields for permission
    for (( i = 0; i < ${#rules[@]}; i++ ))
    do
        # 0, 11, 22
        rule=("${rules[@]:${i}:11}")
        port_min=${rule[5]}
        port_max=${rule[6]}
        if [ "${pmin}" == "${port_min}" ] && [ "${pmax}" == "${port_max}" ]; then
            IA_logerr "Found ingress rule for ${vpc} group ${sgrp}, port ${pmin} ~ ${pmax}"
            return 0
        fi
        let i=${i}+11
    done
    IA_logerr "Did not find any ingress rule for ${vpc} group ${sgrp}, port ${pmin} ~ ${pmax}"
    return 1
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
    IA_find_sgroup_ingress_rule ${sgrp_vpc} ${_RET} ${IA_SGROUP_SSH_PORT}
    if [ $? -ne 0 ];then
        IA_loginfo "Creating inbound cluster SSH rule for ${sgrp_name}..."
        ec2-authorize ${IA_EC2_OPTS} ${_RET} --protocol tcp --port-range 22 --cidr "${sgrp_cidr}"
    fi
    IA_find_sgroup_ingress_rule ${sgrp_vpc} ${_RET} ${IA_SGROUP_HADOOP_PORT_MIN} ${IA_SGROUP_HADOOP_PORT_MAX}
    if [ $? -ne 0 ];then
        IA_loginfo "Creating inbound cluster SSH rule for ${sgrp_name}..."
        ec2-authorize ${IA_EC2_OPTS} ${_RET} --protocol tcp --port-range ${IA_SGROUP_HADOOP_PORT_MIN}-${IA_SGROUP_HADOOP_PORT_MAX} --cidr "${sgrp_cidr}"
    fi
    return 0
}

# find the subnet for the target cluster
# Returns 0 for success, 1 for failure
# Sets _RET the subnet id
function IA_find_subnet()
{
    _RET=`ec2-describe-subnets ${IA_EC2_OPTS} -F "tag:Name=${1}" -F "vpc-id=${2}" -F "cidr=${3}" | grep SUBNET | awk '{print $2}'`
    if [ -z "${_RET}" ]; then
    # try w/o name tag
        _RET=`ec2-describe-subnets ${IA_EC2_OPTS} -F "vpc-id=${2}" -F "cidr=${3}" | grep SUBNET | awk '{print $2}'`
        if [ -z "${_RET}" ]; then
            IA_logerr "Did no find the subnet ${1} for vpc ${2} with CIDR ${3}"
            return 1
        fi
        # found it w/o the tag, so add the tag
        IA_add_name_tag ${_RET} ${1}
    fi
    # found the subnet, let's add the tag
    _RET=${_RET}
    return 0
}

# Get the instance status value from ec2-describe-instance-status, e.g.
#INSTANCE        i-d39edfe7      us-west-2c      running 16      ok      ok      active  (nil)
#SYSTEMSTATUS    reachability    passed  (nil)
#INSTANCESTATUS  reachability    passed  (nil)
function IA_get_instance_status()
{
    local ins=($@)
    local type=${ins[$((${#ins[@]}-1))]}
    case ${type} in
    instance-state-name)
        echo ${ins[3]}
        ;;
    instance-state-code)
        echo ${ins[4]}
        ;;
    instance-status)
        echo ${ins[5]}
        ;;
    system-status)
        echo ${ins[6]}
        ;;
    # these two are in sub-columns
    instance-reachability | system-rechability)
        echo ${ins[3]}
        ;;
    *)
        ;;
    esac
}

# Check a given instance' status by its id
# Returns 0 for success, 1 for failure
# Sets _RET to be "passed" or "failed"
function IA_check_instance_status()
{
    local inst=(`ec2-describe-instance-status ${IA_EC2_OPTS} --show-empty-fields ${1} | grep ${1}`)
    local sysr=(`ec2-describe-instance-status ${IA_EC2_OPTS} --show-empty-fields ${1} | grep SYSTEMSTATUS`)
    local insr=(`ec2-describe-instance-status ${IA_EC2_OPTS} --show-empty-fields ${1} | grep INSTANCESTATUS`)
    local state

    _RET="failed"
    state=`IA_get_instance_status ${inst[@]} instance-state-name`
    if [ "running" != "${state}" ]; then
        IA_loginfo "Instance ${1} instance-state-name is \"${state}\", not \"running\"!"
        return 1
    fi
    state=`IA_get_instance_status ${inst[@]} instance-status`
    if [ "ok" != "${state}" ]; then
        IA_loginfo "Instance ${1} instance-status is \"${state}\", not \"ok\"!"
        return 1
    fi
    state=`IA_get_instance_status ${inst[@]} system-status`
    if [ "ok" != "${state}" ]; then
        IA_loginfo "Instance ${1} system-status is \"${state}\", not \"ok\"!"
        return 1
    fi
    state=`IA_get_instance_status ${insr[@]} instance-reachability`
    if [ "passed" != "${state}" ]; then
        IA_loginfo "Instance ${1} instance-status.reachability is \"${state}\", not \"passed\"!"
        return 1
    fi
    state=`IA_get_instance_status ${sysr[@]} system-rechability`
    if [ "passed" != "${state}" ]; then
        IA_loginfo "Instance ${1} system-status.reachability is \"${state}\", not \"passed\"!"
        return 1
    fi
    IA_loginfo "Instance ${1} status check good."
    _RET="passed"
    return 0
}

# check multiple instances status
# Returns 0 for success, 1 for failure
function IA_check_instances_status()
{
    local iids=($@)

    # check instance status, max 5 waits, every wait is 10s
    for (( i = 0; i < ${#iids[@]}; i++ ))
    do
        # polling wellness of the instances
        IA_loginfo "Checking instance ${1} status..."
        IA_check_instance_status ${iids[${i}]}
        if [ $? -ne 0]; then
            return 1
        fi
    done
    return 0
}

function IA_find_subnet_state()
{
    echo `ec2-describe-subnets ${IA_EC2_OPTS} ${1} -F "vpc-id=${2}" -F "cidr=${3}" | grep SUBNET | awk '{print $3}'`
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
    local snet_state

    IA_loginfo "Looking for subnet ${snet_name}..."
    IA_find_subnet ${snet_name} ${snet_vpc} ${snet_cidr}
    if [ $? -eq 0 ] && [ ! -z "${_RET}" ]; then
        IA_loginfo "Found existing subnet ${snet_name} with id ${_RET} in vpc ${snet_vpc}"
        snet_state=`IA_find_subnet_state ${_RET} ${snet_vpc} ${snet_cidr}`
        if [ "${snet_state}" != "available" ]; then
            IA_logerr "Existing subnet ${snet_name} with id ${_RET} in vpc ${snet_vpc} is in a wrong state as ${snet_state}!"
            return 1
        fi
        return 0
    fi
    # default zone
    IA_loginfo "Creating subnet ${snet_name}..."
    _RET=`ec2-create-subnet ${IA_EC2_OPTS} --vpc ${snet_vpc} --cidr ${snet_cidr} | awk '{print $2}'`
    if [ -z "${_RET}" ]; then
        IA_logerr "Failed to create subnet ${snet_name} with id ${_RET} in vpc ${snet_vpc}!"
        return 1
    fi 

    # we have a subnet id up to here, check the state   
    # check sunet status before moving on, max five times
    IA_loginfo "Created subnet subet (${snet_name}, ${_RET})..."
    for (( i = 0; i < 5; i++ ))
    do
        IA_loginfo "Checking subnet (${snet_name}, ${_RET}) state..."
        snet_state=`IA_find_subnet_state ${_RET} ${snet_vpc} ${snet_cidr}`
        if [ "${snet_state}" == "${IA_AWS_AVAILABLE}" ]; then
            IA_add_name_tag ${_RET} ${snet_name}
            return 0
        fi
        sleep 5s
    done

    IA_logerr "Created subnet ${sne_name} for VPC ${snet_vpc} using CIR ${snet_cidr}, but in a wrong state \"${snet_state}\"!"
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

    IA_loginfo "Updating route table ${rt_id} in vpc ${rt_vpc} for subnet ${rt_snet}..."
    _RET=`IA_find_routetable_subnet ${rt_id} ${rt_snet}`
    if [ ! -z "${_RET}" ] && [ "${_RET}" == "${rt_snet}" ]; then
        IA_loginfo "Found existing association in route table ${rt_id} for subnet ${_RET}"
        return 0
    fi
    _RET=`ec2-associate-route-table ${IA_EC2_OPTS} ${rt_id} --subnet ${rt_snet} | awk '{print $4}'`
    if [ ! -z "${_RET}" ] && [ "${_RET}" == "${rt_snet}" ]; then
        IA_loginfo "Updated route table with association with subnet ${rt_snet}, RET=${_RET}"
        return 0
    fi
    IA_logerr "Failed to associate subnet ${rt_snet} with routing table ${rt_id}, RET=${_RET}!"
    _RET=""
    return 1
}

function IA_get_instance_column_value()
{
    local ins=($@)
    local type=${ins[$((${#ins[@]}-1))]}
    case ${type} in
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
    instance-state-name)
        echo ${ins[5]}
        ;;
    key-name)
        echo ${ins[6]}
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

function IA_get_instance_column_by_filter()
{
    local filter=$1
    local ins=(`ec2-describe-instances ${IA_EC2_OPTS} --show-empty-fields -F "${filter}" | grep INSTANCE`)
    
    echo `IA_get_instance_column_value ${ins[@]} $2`
} 

# get the value of a given column for an ec2 instance
# this is extracted from ec2-describe-instances, and matches the ec2 docs 
# supported filter fields
function IA_get_instance_column()
{
    echo `IA_get_instance_column_by_filter "tag:Name=${1}" $2`
}

# find out the ec2 instance id by the name tag
function IA_get_instance_id_by_vpc()
{
    local name=$1
    local vpc=`IA_get_vpc`

    echo `ec2-describe-instances ${IA_EC2_OPTS} -F "vpc-id=${vpc}" -F "tag:Name=${name}" | grep INSTANCE | awk '{print $2}' `
    return 0
}

function IA_get_instance_id()
{
    echo `IA_get_instance_column $1 instance-id`
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
    local cid=$1
    local csize=$2
    local outdir=$3

    if [ ! -d ${outdir} ]; then
        IA_logerr "Output director \"${outdir}\" not found!"
        return 1
    fi
    headers=${outdir}/headers.hosts
    if [ ! -f ${headers} ]; then
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
        echo "${dnsfull[$i]}" >> ${outnodes}
    done
    IA_loginfo "Generated hosts file ${outhosts}..."
    IA_loginfo "Generated nodes file ${outnodes}..."
}
