#!/bin/bash
DISKS=(xvdb xvdc xvdd xvde)
let j=0
for ((i = 0; i < ${#DISKS[${i}]}; i++ ))
do
	d="${DISKS[${i}]}"
	let j=$i+1
	uuid=`sudo blkid /dev/${d} | sed -e "s/^.*UUID=//g" -e "s/\"//g" -e "s/TYPE.*//g"`;
	echo -e "UUID=${uuid}\t/mnt/data${j}\text4\tdefaults,user\t0 2"
 # /etc/fstab
	echo `hostname`:added ${d} to fstab...
done
# mount volumes
mount -a
chown -R ${USER}.${USER} /mnt/data*

