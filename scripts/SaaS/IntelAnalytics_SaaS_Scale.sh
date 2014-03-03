#!/bin/bash
#Creates a new autoscaling group, launch config, scaling polices and alarms
# requires
#	aws cli to interface with aws
#   jq cli to parse json from aws commands
# The graphtrial.intel.com site runs with in an auto scaling group to make sure we maintain the desired number of machines behind the load
# balancer and to auto scale based on demand. Manually creating and attaching scaling policies is tedious and potentially error prone. This script automates
# the creation of an auto scaling group, launch config, scaling policies and alarms. If any changes need to be deployed to
# graphtrial.intel.com such as css,js, or scala play changes first deploy the play dist package to s3://gaoprivate/SaaS/
# then run this script with an approriate launch-config-name ie graphtrial.intel.comvSPRINT and watch the magic happen
#
INSTANCE_TYPE="m1.medium"
IMAGE_ID="ami-b06a0b80"
KEY_NAME="IntelAnalytics-SaaS-Admin"
SCALE_MIN=1
SCALE_MAX=20
LOAD_BALANCER="IntelAnalytics-SaaS-pre-prod"
ALARM_ARN_NOTIFICATION="arn:aws:sns:us-west-2:953196509655:bdaawssupport"

TEMP=`getopt -o l: --long launch-config-name:,instance-type:,image-id:,key-name:,scale-min:,scale-max:,load-balancer:,alarm-notification: -n 'IntelAnalytics_SaaS_AutoScale.sh' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

eval set -- "$TEMP"

while true; do
    case "$1" in
		--launch-config-name)
			echo "Option launch-config-name, argument '$2'"
			launchConfigName=$2
			shift 2 ;;
		--instance-type)
			echo "Option instance-type, argument '$2'"
			INSTANCE_TYPE=$2
			shift 2 ;;
		--image-id)
			echo "Option image id, argument '$2'"
			IMAGE_ID=$2
			shift 2 ;;
		--key-name)
			echo "Option key-name, argument '$2'"
			KEY_NAME=$2
			shift 2 ;;
		--scale-min)
			echo "Option scale-min, argument '$2'"
			SCALE_MIN=$2
			shift 2 ;;
		--scale-max)
			echo "Option scale-max, argument '$2'"
			SCALE_MAX=$2
			shift 2 ;;
		--load-balancer)
			echo "Option load-balancer, argument '$2'"
			LOAD_BALANCER=$2
			shift 2 ;;
		--alarm-notification)
			echo "Option alarm-notification, argument '$2'"
			LOAD_BALANCER=$2
			shift 2 ;;
		--) shift ; break ;;
		*) echo "Internal error!" ; exit 1 ;;
    esac
done

echo "To continue type 'yes': "
read -s CONTINUE
if [ "$CONTINUE" != "yes" ]
then
	echo " params not accepted"
	exit 0
fi

echo "create autos caling group"
aws autoscaling create-launch-configuration --launch-configuration-name "$launchConfigName" \
	--instance-type $INSTANCE_TYPE --image-id $IMAGE_ID --key-name $KEY_NAME \
	--security-groups "sg-191d177b" "sg-d51d17b7" --associate-public-ip-address

echo "launch auto scaling group"
aws autoscaling create-auto-scaling-group --launch-configuration-name "$launchConfigName" \
	--auto-scaling-group-name "$launchConfigName" --min-size $SCALE_MIN --max-size $SCALE_MAX --desired-capacity $SCALE_MIN \
	--load-balancer-names "$LOAD_BALANCER" --health-check-type ELB --health-check-grace-period 300 \
	--vpc-zone-identifier "subnet-b50e36c1,subnet-78060f1a,subnet-881c46ce"

echo "create up scaling policy for $launchConfigName"
UP=$(aws autoscaling put-scaling-policy --auto-scaling-group-name "$launchConfigName" --policy-name "up" --scaling-adjustment 2 --adjustment-type ChangeInCapacity --cooldown 300 | jq '.PolicyARN' )
up=$UP
#trim quotes
up=${up%\"}
up=${up#\"}

echo "attach metrics to up scaling policy"
aws cloudwatch put-metric-alarm --alarm-name "$launchConfigName-UP" \
	--alarm-description "add more graphtrial.intel.com instances" --metric-name "CPUUtilization" \
	--namespace "AWS/EC2" --statistic "Average" --period 300 --evaluation-periods 1 --threshold 70.0 \
	--comparison-operator GreaterThanOrEqualToThreshold --dimensions  Name=AutoScalingGroupName,Value=$launchConfigName \
	--alarm-actions "$ALARM_ARN_NOTIFICATION" "$up"


echo "create DOWN scaling policy for $launchConfigName"
DOWN=$(aws autoscaling put-scaling-policy --auto-scaling-group-name "$launchConfigName" --policy-name "down" --scaling-adjustment -1 --adjustment-type ChangeInCapacity --cooldown 300 | jq '.PolicyARN' )
down=$DOWN
down=${down%\"}
down=${down#\"}

echo "attach metrics to down scaling policy"
aws cloudwatch put-metric-alarm --alarm-name "$launchConfigName-DOWN" \
	--alarm-description "remove graphtrial.intel.com instances" --metric-name "CPUUtilization" \
	--namespace "AWS/EC2" --statistic "Average" --period 300 --evaluation-periods 1 --threshold 10.0 \
	--comparison-operator LessThanOrEqualToThreshold --dimensions  Name=AutoScalingGroupName,Value=$launchConfigName \
	--alarm-actions "$ALARM_ARN_NOTIFICATION" "$down"
