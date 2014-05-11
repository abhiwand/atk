#!/bin/bash
#get minion id script

#start on filesystem and net-device-up IFACE=eth0

#stop on runlevel [016]
ntpdate pool.ntp.org

export AWS_ACCESS_KEY_ID="AKIAJCPOW44SGKNVDALQ"
export AWS_SECRET_ACCESS_KEY="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"
export AWS_DEFAULT_REGION="us-west-2"

	awsMetaIp=169.254.169.254
	instanceId=$(GET http://$awsMetaIp/latest/meta-data/instance-id)

	MINIONID=""
	instanceDetails=$(aws ec2 describe-instances --instance-ids $instanceId | jq -c '.Reservations[].Instances[0]')
	amiLaunchIndex=$(echo $instanceDetails | jq -c '.AmiLaunchIndex')

	tags=$(echo $instanceDetails | tr -d " "  | jq  -c '.Tags[]')

	autoScaleGroups=$(aws autoscaling describe-auto-scaling-groups |  tr -d " " | jq -c  '.AutoScalingGroups[]' )

autoScalingGroup=""
function inAutoScalingGroup()
{
    local instanceIdToCheck=$1
    for autoScalingGroup in $autoScaleGroups
    do
        local autoScalingGroupInstances=$(echo $autoScalingGroup | jq -c '.Instances[]' )
        for instance in $autoScalingGroupInstances
        do
            local instanceId=$(echo $instance | jq -r -c '.InstanceId')
            if [ "$instanceId" == "$instanceIdToCheck" ]; then
                autoScalingGroup=$autoScalingGroup
                return 1
            fi
        done
    done
    return 0
}


#see if we have a MinionId tag to derive our minion id from
for tag in $tags
do
    
    key=$(echo $tag | jq -c -r '.Key')
    value=$(echo $tag | jq -c -r '.Value')
    if [ "$key" == "minionId" ]; then
        MINIONID=$value
    fi   
done
echo "derived from minion tag"
echo $MINIONID

#MINIONID=""
#if no MinionId tag exists we will fall back to Name tag + ami launch index
#if it's not part of an auto scaling groups we will drop ami launch index
if [ "$MINIONID" == "" ]; then
    for tag in $tags
    do
        
        key=$(echo $tag | jq -c -r '.Key')
        value=$(echo $tag | jq -c -r '.Value')
        if [ "$key" == "Name" ] && [ "$value" != "" ] && [ "$value" != "null" ]; then
           
            inAutoScalingGroup "$instanceId"
            minSize=$(echo $autoScalingGroup | jq -c -r '.MinSize')
            maxSize=$(echo $autoScalingGroup | jq -c -r '.MaxSize')
            desiredCapacity=$(echo $autoScalingGroup | jq -c -r '.DesiredCapacity')
            if [ $? -eq 1 ] && [ $minSize -ne 1  ] && [ $maxSize -ne 1 ] && [ $desiredCapacity -ne 1 ]; then
                    MINIONID="$value-$amiLaunchIndex"
                else
                    MINIONID="$value"
            fi
        fi
    done
fi
echo "derived from name tag"
#echo $MINIONID

#if no name tag exists we will fall back auto scale name + ami launch index
#if the auto scale groups is a 1,1,1 min,max,desired config ami launch index will be dropped.
MINIONID=""
if [ "$MINIONID" == "" ]; then
    inAutoScalingGroup $instanceId
    if [ $? -eq 1 ]; then
        autoScalingGroupName=$(echo $autoScalingGroup | jq -c -r '.AutoScalingGroupName')
        minSize=$(echo $autoScalingGroup | jq -c -r '.MinSize')
        maxSize=$(echo $autoScalingGroup | jq -c -r '.MaxSize')
        desiredCapacity=$(echo $autoScalingGroup | jq -c -r '.DesiredCapacity')
        if [ $minSize -ne 1  ] && [ $maxSize -ne 1 ] && [ $desiredCapacity -ne 1 ]; then
                MINIONID="$autoScalingGroupName-$amiLaunchIndex"
            else
                MINIONID="$autoScalingGroupName"
        fi    
        
    fi
fi
echo "derived from auto scaling name"
echo $MINIONID

#MINIONID=""
#if it's not part of an auto scaling group and the tag name is empty wi will use the vpc name/id + instance-id
if [ "$MINIONID" == "" ]; then
    vpcId=$(echo $instanceDetails | jq -c -r '.VpcId')
    vpcTags=$(aws ec2 describe-vpcs --vpc-ids $vpcId --filters "Name=tag-key,Values=Name" | jq -c -r '.Vpcs[0].Tags[]')
    for tag in $vpcTags
    do
        key=$(echo $tag | jq -c -r '.Key')
        value=$(echo $tag | jq -c -r '.Value')
        if [ "$key" == "Name" ]; then
            MINIONID="$value-$instanceId"
        fi
    done
fi
echo "derived from vpc name and instance id"
echo $MINIONID

sed -i "s/^#id:.*/id: ${MINIONID}/g" /etc/salt/minion
sed -i "s/^#master:.*/master: build.graphtrial.infra-host.com/g" /etc/salt/minion
sed -i "s/^master:.*/master: build.graphtrial.infra-host.com/g" /etc/salt/minion
initctl stop salt-minion
initctl start salt-minion
