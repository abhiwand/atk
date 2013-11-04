#!/bin/bash
 
#
# Big WIP: this is only the workflow description
#
# Implementation script will follow
#
# Note !! must have right EC2 env
# TODO:
# - support rolling back? or just manual clean on error
# - support VPC network acl?
# - support dryrun
# - assume some resources, e.g., per vpc sec groups, are properly defined
# - supports default 4 data volumes from IS, can be extended by ec2-register to add more
# - --iam-profile
#
function usage()
{
    IA_logerr "Usage:$(basename $0) --cluster-id <id> [--cluster-size <n>] [--no-dryrun]"
    exit 1
}

function IA_create_dump()
{
    IA_loginfo "Cluster Name = ${cname}"
    IA_loginfo "  Time Stamp      = `date`"
    IA_loginfo "  Assigned VPC    = ${cvpcid}"
    IA_loginfo "  AMI Image ID    = ${camiid}"
    IA_loginfo "  Target CIDR     = ${ccidr}"
    IA_loginfo "  Target Subnet   = ${csubnet}"
    IA_loginfo "  IAM Group:User  = ${ciamgroup}:${ciamuser}"
    IA_loginfo "  Secuirty Groups = ${csgroup},${csgroup_https},${csgroup_admssh}"
    IA_loginfo "  Placement Group = ${cpgroup}"
    IA_loginfo "  Route Table     = ${croute}"
    IA_loginfo "  Cluster Nodes   = ${nnames[@]}"
    IA_loginfo "  OutputCluster Nodes   = ${nnames[@]}"
}

# Get the env setup and helper funs
source IntelAnalytics_cluster_env.sh

# Reset the global RET
_RET=""
dryrun=yes
#et per cluster log

# Check inputs
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
          --no-dryrun)
            dryrun=no
            shift 1
            ;;
        *)
            usage
            ;;        
    esac
done

# Input 1 is a unique cluster id from frontend user registration
IA_check_cluster_id ${cid};
if [ $? -ne 0 ]; then
    IA_logerr "Invalid input cluster id ${cid}!"
    usage
fi

# Input 2 is the number of nodes in the cluster
if [ -z "${csize}" ]; then
    csize=${IA_CLUSTER_SIZE_MIN}
    IA_loginfo "Setting to default cluster size ${csize}!"
fi
IA_check_cluster_size ${csize};
if [ $? -ne 0 ]; then
    IA_logerr "Invalid input cluster size ${csize}!"
    usage
fi

# Find out the cidr from the given cid
IA_check_cluster_cidr ${cid}
if [ $? -ne 0 ]; then
    IA_logerr "Invalid input cluster CIDR!"
    usage
fi
ccidr="${_RET}"

# Prefix=IntelAnalytics-${id}
cname=`IA_format_cluster_name "${cid}-${csize}"`
IA_logfile "${IA_CLUSTERS}/${cname}.log"
IA_loginfo "`date`: preparing to create cluster ${cname} (${cid},${csize},${ccidr})..."
IA_loginfo "Cluster basic info:id=${cid}, size=${csize}, CIDR=${ccidr}"
IA_loginfo "Log file at ${IA_LOGFILE}"

## No difference between master and slave any more!!!
## Retrieve cluster node AMI image id for master node
#camiid=`IA_get_ami "${IA_AMI_MASTER}"`
#if [ -z "${camiid}" ]; then
#    IA_logerr "No AMI ID found for image \"${IA_AMI_MASTER}\" found!"
#    exit 1
#fi
#
## Retrieve cluster node AMI image id for slave node
#cslave=`IA_get_ami "${IA_AMI_SLAVE}"`
#if [ -z "${cslave}" ]; then
#    IA_logerr "No AMI ID found for image \"${IA_AMI_SLAVE}\" found!"
#    exit 1
#fi

# Retrieve cluster node AMI image id, will use the same image
camiid=`IA_get_ami "${IA_AMI_NODE}"`
if [ -z "${camiid}" ]; then
    IA_logerr "No AMI ID found for image \"${IA_AMI_NODE}\" found!"
    exit 1
fi
IA_loginfo "AMI IMAGE = ${camiid}"

# Retrieve instance type
cinstype=`IA_get_instance_type`
if [ -z "${cinstype}" ]; then
    IA_logerr "No valid instance type defined for cluser \"${cname}\"!"
    exit 1
fi
IA_loginfo "Instance Type = ${cinstype}"

# Retrieve cluster vpc id
cvpcid=`IA_get_vpc`
if [ -z "${cvpcid}" ]; then
    IA_logerr "No VPC found for cluser \"${cname}\" found!"
    exit 1
fi
IA_loginfo "VPC ID = ${cvpcid}"

# Retrieve cluster Group IAM
ciamgroup=`IA_get_iamgroup`
if [ -z "${ciamgroup}" ]; then
    IA_logerr "No AMI ID found for image \"${IA_AMI_SLAVE}\" found!"
    exit 1
fi
IA_loginfo "IAM group = ${ciamgroup}"

# Retrieve cluster User IAM
ciamuser=`IA_get_iamuser`
if [ -z "${ciamuser}" ]; then
    IA_logerr "No AMI ID found for image \"${IA_AMI_SLAVE}\" found!"
    exit 1
fi
IA_loginfo "IAM user = ${ciamuser}"

# Retrieve the associated route table
croute=`IA_get_routetable ${cname} ${cvpcid}`
if [ -z "${croute}" ]; then
    IA_logerr "No route table found for VPC ${cvpcid}"
    exit 1
fi
IA_loginfo "Route table = ${croute}"

# Create cluster placement group
IA_create_pgroup ${cname}
if [ $? -ne 0 ] || [ -z "${_RET}" ]; then
    IA_logerr "Failed to create placemment group for ${cname}!"
    exit 1
fi
cpgroup=${_RET}
IA_loginfo "Placement group = ${cpgroup}"

# Create cluster subnet matching the input CIDR
IA_create_subnet ${cname} ${cvpcid} "${ccidr}"
if [ $? -ne 0 ] || [ -z "${_RET}" ]; then
    IA_logerr "FaiLed to create subnet for ${ccidr}!"
    exit 1
fi
csubnet=${_RET}
IA_loginfo "Subnet id = ${csubnet}"

# Get the HTTPS security group for iPython/master node
csgroup_https=`IA_get_sgroup_https ${cname} ${cvpcid}`
if [ -z "${csgroup_https}" ]; then
    IA_logerr "No HTTPS security group found for VPC ${cvpcid}!"
    exit 1
fi
IA_loginfo "Security group (HTTPS) = ${csgroup_https}"

# Get the ADMIN SSH security group for iPython/master node
csgroup_admssh=`IA_get_sgroup_admssh ${cname} ${cvpcid}`
if [ -z "${csgroup_admssh}" ]; then
    IA_logerr "No Admin SSH security group found for VPC ${cvpcid}!"
    exit 1
fi
IA_loginfo "Security group (Adm SSH) = ${csgroup_admssh}"

# Create per cluster security groups
IA_create_sgroup ${cname} ${cvpcid} ${ccidr}
if [ $? -ne 0 ] || [ -z "${_RET}" ]; then
    IA_logerr "Failed to create security group for cluster ${cname} in VPC ${cvpcid}!"
    exit 1
fi
csgroup=${_RET}
IA_loginfo "Security group (SSH) = ${csgroup}"

# Associate cluster subnet to VPC router
IA_update_routes ${croute} ${cvpcid} ${csubnet}
if [ $? -ne 0 ] || [ -z "${_RET}" ]; then
    IA_logerr "Failed to update the routing table ${croute} for cluster ${cname} in VPC ${cvpcid}!"
    exit 1
fi

# TODO:
# Prepare the user data script
# - Prepare ssh keypair for password less login
# - Prepare cluster hosts file password less login
# - Format IS disks
# - Mount IS disks to /mnt/data{1,4}
# - update cluster config
# - bring up the cluster

# - Launch 4 instances into the placement group
#nname=`IA_format_node_name ${cname} ${i}`
nnames=(
"`IA_format_node_name ${cname} 0`" 
"`IA_format_node_name ${cname} 1`" 
"`IA_format_node_name ${cname} 2`" 
"`IA_format_node_name ${cname} 3`")

# dump 
IA_create_dump

for (( i = 0; i < ${csize}; i++ ))
do
    nname=${nnames[$i]}

    cmd_opts="${IA_EC2_OPTS} ${camiid} \
--instance-count 1 \
--key ${ciamuser} \
--group ${csgroup} \
--group ${csgroup_admssh} \
--instance-type ${cinstype} \
--placement-group ${cpgroup} \
--subnet ${csubnet}"
    
    if [ $i -eq 0 ]; then
        cmd_opts="${cmd_opts} --group ${csgroup_https} --associate-public-ip-address true"
    fi
    IA_loginfo "Creating EC2 instance for node ${nname}, executing..."
    IA_loginfo "  ec2-run-instances ${cmd_opts}"

    # set tag
    if [ "${dryrun}" == "no" ]; then
        iid=`ec2-run-instances ${cmd_opts} | grep INSTANCE | awk '{print $2}'`
	    IA_add_name_tag ${iid} ${nname}
	    IA_check_instance_status ${iid}
    fi
done

# generate a report
cat << EOF > ${IA_CLUSTERS}/${cname}.info

Time Stamp      = `date`
Cluster Name    = ${cname}
Assigned VPC    = ${cvpcid}
AMI Image ID    = ${camiid}
Target CIDR     = ${ccidr}
Target Subnet   = ${csubnet}
IAM Group:User  = ${ciamgroup}:${ciamuser}
Secuirty Groups = ${csgroup},${csgroup_https},${csgroup_admssh}
Placement Group = ${cpgroup}
Route Table     = ${croute}
Cluster Nodes   = ${nnames[@]}
EC2 Creation Commandline Options: ${cmd_opts}

EOF

# generate hosts file
if [ "${dryrun}" == "no" ]; then
    IA_generate_hosts_file ${cid} ${csize} ${IA_CLUSTERS}
fi
