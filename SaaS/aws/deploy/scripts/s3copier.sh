#!/bin/bash
#monitor sqs for new file uplads that are waiting for us on s3.
#the root user needs to have aws cli installed and configured
#jq: json query cli also needs to be installed

#just inscase the root user doesn't have the aws cli configured i'm going to copy the config from ec2-user
mkdir /root/.aws
cp /home/ec2-user/.aws/config /root/.aws/config

#get the access and  secret
access=$(cat /home/ec2-user/.aws/config | grep aws_access_key_id | awk -F" " '{print $3}')
secret=$(cat /home/ec2-user/.aws/config | grep aws_secret_access_key | awk -F" " '{print $3}')

#get this machines instance-id
instanceId=$(curl http://169.254.169.254/latest/meta-data/instance-id)
echo "instance id: $instanceId"

#the sqs queue url
queue=$(aws sqs list-queues --queue-name-prefix $instanceId | jq -c -r '.QueueUrls[0]')
echo "queue url: $queue"

#get a message from the queue if no message is available wait 20 sec for a message. 20 seconds is the max
#once the message is picked it won't be visibile for another 200 seconds.
message=$(aws sqs receive-message --wait-time-seconds 20 --visibility-timeout 20 --queue-url $queue |  jq -r '.Messages[0]')

#if we go no messages exit the script
if [ "$message" == "" ] || [ "$message" == "null"  ];then
		exit 0
	else
		echo "message: $message"
fi

#get the url of the message to delete the queue later
receipt=$(echo $message | jq -c -r '.ReceiptHandle' )
#parse the queue body that was inserted by frontend app
body=$(echo $message | jq -r '.Body')

#the bucket we will copy from
bucket=$(echo $body | jq -c -r '.create.bucket')
#the path fo the file in the bucket
path=$(echo $body | jq -c -r '.create.path')
#base file name
fileName=${path##*/}

size=$(echo $body | jq -c -r '.create.size')

echo "bucket: $bucket"
echo "path: $path"
echo "size: $size"

TIMESTAMP=$(date --utc +%Y%m%d%H%M%SZ)

#s3distcp job to copy the file form s3 to hdfs
nohup runuser -l hadoop -c "hadoop distcp2 -log /tmp/filetransfer$TIMESTAMP s3n://$access:$secret@$bucket/$path hdfs:/user/hadoop/uploadedFiles/$fileName " &

progress=0

while [ $progress -ne 1  ]
do
	#ls hadoop to see if the file is copied over
	hfileInfo=$(runuser -l hadoop -c "hadoop fs -ls /user/hadoop/uploadedFiles/$fileName ")
	#get the file size
	fileCopyProgress=$(echo $hfileInfo | awk -F" " '{print $8}')
	
	if [ $fileCopyProgress ]; then

		echo $fileCopyProgress
		#device and preserve decimals
		lprogress=`echo "($fileCopyProgress/$size) " | bc -l`
		lprogress=`echo "$lprogress * 100" | bc -l`
		#write a file with json data so the front end can monitor the progress and report to the user
		echo "{\"name\":\"/$fileName\",\"progress\":$lprogress}" > $fileName.status
		aws s3 cp $fileName.status  s3://$bucket/$instanceId/$fileName.status
		#interger division
		progress=`echo " $fileCopyProgress/$size" | bc `
	fi
	echo $progress
sleep 1
done

#if the file made to hdfs delete the queue
if [ $progress -eq 1 ]; then
	aws sqs delete-message --queue-url $queue --receipt-handle $receipt
fi
