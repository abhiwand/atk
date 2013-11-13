#!/bin/bash
# Notes: executed on the cluster nodes
# This script is not necessarily needed since we are only
# going to mount these data disks once, since no instance
# will ever be rebooted
IA_DISKS=(xvdb xvdc xvdd xvde)
IA_USR=hadoop

let j=0
for ((i = 0; i < ${#IA_DISKS[@]}; i++ ))
do
	d="${IA_DISKS[${i}]}"
	let j=$i+1
	uuid=`sudo blkid /dev/${d} | sed -e "s/^.*UUID=//g" -e "s/\"//g" -e "s/TYPE.*//g"`;
	echo -e "UUID=${uuid}\t/mnt/data${j}\text4\tdefaults,user\t0 2"
 # /etc/fstab
	echo `hostname`:added ${d} to fstab...
done
# mount volumes
mount -a
chown -R ${IA_USR}.${IA_USR} /mnt/data*

