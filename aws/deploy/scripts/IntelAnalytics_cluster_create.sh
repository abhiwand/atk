#!/bin/bash
# This is the core script that does the cluster creation. It shows the workflow
# of bringing up a cluster on AWS inside the given target VPC
#
# Notes:
# - Most core functions are in _cluster_env.sh script
# - This assumes to work on the existing VPC, if not, use the env variable to override
#    IA_TAG, the VPC name tag, default to "IntelAnalytics" used to get to the VPC id
#    IA_EC2_USR, amdin user, default to ${IA_NAME}_Adm, must have ${IA_EC2_USER}.csv/.pem in ${IA_HOME}/credentials
#    IA_AWS_REGION: region, default to us-west-2
#    IA_SUBNET, the flat L2 sunet for this VPC, default to 10.0.0.0/18
#    IA_IP_ADMIN, the admin node private ip for this VPC
#    IA_ROUTETABLE, the route table id for this VPC
#    IA_SGROUP_HTTPS, the default HTTPS security group for this VPC
#    IA_SGROUP_ADMSSH, the default Adm SSH access security group for this VPC
# - You must have the most recent EC2 CLI to allow this scrip to work, particularly the
#   ec2-run-instances needs the latest version to support public ip enabling option
# - This currently does not support rolling back to delete resources created but failed to
#   get started
# - By default this only does a fake dry-run, "fake" as it is still only create some resources
#   but dry-run refers to no creation of the actual EC2 instances
# - This assume the basic resources of the VPC are there, and are hard-coded to existing VPC
#   "IntelAnalytics".
# - This current supports 4, 8, 12, 16, 20 nodes. The subnet per cluster is 5 bits but varies
#   in total avaialbel private IP depending on the reserved addresses
# - All nodes are built based on our preexisting master node AMI that has the current agreed
#   server components version, hadoop-1.2.1, hbase-0.94.12, titan-all-0.4.0
#
# Further Technical Notes:
# - This is not necessary since we have post configuration scripts in place, but ideally, we should
#   use AWS user data script to do per EC2 instance boot time configuration, such as disk preparation.
#   However, I haven't had much luck getting it to work to allow files system preparation of data
#   disks on the instance store (ephemoral)
# - We should update per cluster user hadoop's default ssh keypair, currently all clusters are
#   created w/ the same default ssh keypair.
# - For slave nodes instances, should try to create them all at once, to allow a better chance to be
#   fit into the same placement group. The reason we are not doing that was that we may be able to
#   assign the static private ip for each instance, allowing up to prebuild the hosts file before
#   creating the instance. However, it seesm quite difficult to figure out which IPs can be used,
#   where AWS seems to reserver some of the outside the stand broadcast, loopback ips, so it may
#   be better to just create all slave instances once.

function usage()
{
    echo "Usage: $(basename $0) 
    --cluster-id <id>       // valid range for id is [1, 40]
    [--owner "owner" ]      // the owner of the cluster, primary user
    [--cluster-size <n> ]   // default to n=4
    [--build <Build.nn> ]   // build version as "Build.01", "Build.02", etc
    [--version <rev> ]      // rev is the product relsease version, default to 0.5
    [--ec2adm <str> ]       // ec2 adm name, whose adw key is used to issue ec2 commands
    [--workdir <str> ]      // work home, e.g. \`pwd\`/../..
    [--credentials <str> ]  // directory with credentials
    [--use-placement-group] // use placement group for nodes within a cluster
    [--use-public-ip-for-slave ] // allow slave nodes to have public ip
    [--no-dryrun]           // really launch instance
    [--help ]               // print this message
"
    exit 1
}

function IA_create_info()
{
# generate a report
cat << EOF > ${IA_CLUSTERS}/${cname}.info

Time Stamp      = `date`
Release Version = ${IA_VERSION}
Build Verions   = ${IA_AMI_BUILD}
AMI Version     = ${IA_AMI_VERSION}
AMI Image Name  = ${IA_AMI_NODE}
Cluster Name    = ${cname}
Assigned VPC    = ${cvpcid}
AMI Image ID    = ${camiid}
Target CIDR     = ${ccidr}
Target Subnet   = ${csubnet}
IAM Group:User  = ${ciamgroup}:${ciamuser}
Secuirty Groups = ${csgroup},${csgroup_https},${csgroup_admssh}
Placement Group = ${cpgroup}
Route Table     = ${croute}
Cluster Nodes   = ${cnnames[@]}
Cluster Public  = ${cpublic}
Instance IDs    = ${cniids[@]}
Dry Run         = ${dryrun}

EOF
}

function IA_create_dump()
{
    IA_create_info
    cat ${IA_CLUSTERS}/${cname}.info >> ${IA_LOGFILE}
}

# Reset the global RET
_RET=""
dryrun=yes
cpgroup="no"
cpublic="no"

# Check inputs
while [ $# -gt 0 ]
do
    case "$1" in
        --cluster-id)
            cid=$2
            shift 2
            ;;
        --owner)
            owner=$2
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
        --build)
            export IA_AMI_BUILD=$2
            shift 2
            ;;
        --version)
            export IA_VERSION=$2
            shift 2
            ;;
        --ec2adm)
            export IA_EC2_USR=$2
            shift 2
            ;;
        --workdir)
            export IA_HOME=$2
            shift 2
            ;;
        --credentials)
            export IA_CREDENTIALS=$2
            shift 2
            ;;
        --use-placement-group)
            cpgroup="yes"
            shift 1
            ;;
        --use-public-ip-for-slave)
            cpublic="yes"
            shift 1
            ;;
        --help | *)
            usage
            ;;
    esac
done

# Get the env setup and helper funs
source IntelAnalytics_cluster_env.sh

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
if [ "${cpgroup}" == "yes" ]; then
    IA_create_pgroup ${cname}
    if [ $? -ne 0 ] || [ -z "${_RET}" ]; then
        IA_logerr "Failed to create placemment group for ${cname}!"
        exit 1
    fi
    cpgroup=${_RET}
fi
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
IA_loginfo "Security group (in-cluster SSH and TCP) = ${csgroup}"

# Associate cluster subnet to VPC router
IA_update_routes ${croute} ${cvpcid} ${csubnet}
if [ $? -ne 0 ] || [ -z "${_RET}" ]; then
    IA_logerr "Failed to update the routing table ${croute} for cluster ${cname} in VPC ${cvpcid}!"
    exit 1
fi

# - Launch 4 instances into the placement group
for (( i = 0; i < ${csize}; i++ ))
do
    cnnames[$i]=`IA_format_node_name ${cname} $i`
done

echo 
#ready auto scale config options

cmd_lc_opts="/usr/local/bin/aws autoscaling create-launch-configuration  --launch-configuration-name \"${cname}\" \
    --instance-type ${cinstype} --image-id ${camiid} --key-name ${ciamuser} \
    --security-groups '$csgroup' '$csgroup_admssh'  "
cmd_asg_opts="/usr/local/bin/aws autoscaling create-auto-scaling-group  --launch-configuration-name \"${cname}\" \
    --auto-scaling-group-name \"${cname}\" --min-size ${csize} --max-size ${csize} --desired-capacity ${csize} \
    --vpc-zone-identifier \"${csubnet}\"\
    --tags ResourceId=\"${cname}\",ResourceType=\"auto-scaling-group\",Key=\"Name\",Value=\"${cname}\",PropagateAtLaunch=true "

    #check for an ower tag
    if [[ ! -z "$owner" ]]; then
        cmd_asg_opts="${cmd_asg_opts} ResourceId=\"${cname}\",ResourceType=\"auto-scaling-group\",Key=\"Owner\",Value=\"${owner}\",PropagateAtLaunch=true "
    fi
    
    # For master node, open inbound https for master node hosting ipython
    # also allows public ip for master node
    # For slave nodes, if cluster wide public (e.g. for s3ditscp) enable
    # public ip for slave nodes as well
   # if [ $i -eq 0 ]; then
       # cmd_opts="${cmd_opts} --group ${csgroup_https}"
        #IA_loginfo "Will enable inbound https for ${cnnames[$i]}..."
    #fi
    if [ $i -eq 0 ] || [ "${cpublic}" == "yes" ]; then
        cmd_lc_opts="${cmd_lc_opts} --associate-public-ip-address"
        IA_loginfo "Will enable public ip for ${cnnames[$i]}..."
    fi
    # create launch config and auto scale group
    if [ "${dryrun}" == "no" ]; then
        IA_loginfo "Creating launch config ${cname}, executing..."
        IA_loginfo " ${cmd_lc_opts}"
        eval $cmd_lc_opts
        IA_loginfo "Creating scaling group ${cname}, executing..."
	    IA_loginfo " ${cmd_asg_opts}"
        eval $cmd_asg_opts
        IA_add_notifications ${cname} ${csize}
    else
        IA_loginfo "DRYRUN:creating launch config  ${cname}, executing..."
        IA_loginfo " ${cmd_lc_opts}"
        IA_loginfo "DRYRUN:creating scaling group ${cname}, executing..."
        IA_loginfo " ${cmd_asg_opts}"
    fi
#done

# dump
IA_create_dump

# generate hosts file
if [ "${dryrun}" == "no" ]; then
    # polling wellness of the instances, retry up to 5 times
    for (( i = 0; i < 10; i++ ))
    do
        IA_health_check_auto_scale ${cname}
      
        if [ $(($csize * 2 )) -eq $? ]; then
            IA_loginfo "All instances passed health check"
            break
        fi
        sleep 10s
	done

    # now instances are running, create hosts file
    IA_generate_hosts_file_auto_scale ${cid} ${csize} ${cname} ${csgroup_https}  ${IA_CLUSTERS}
fi
