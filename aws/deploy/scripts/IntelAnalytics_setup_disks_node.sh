#!/bin/bash
# Notes: executed on the cluster nodes in sudo
# This script is not necessarily needed since we are only
# going to mount these data disks once, since no instance
# will ever be rebooted
# -- 
IA_DISKS=(xvdb xvdc xvdd xvde)
IA_USR=hadoop
DRYRUN=""
if [ "$1" == "--dry-run" ]; then
    DRYRUN="echo "
fi
# the AMI default mounts xvdb
# create fs for each data disks
for ((i = 0; i < ${#IA_DISKS[@]}; i++ ))
do
    mount | grep xvdb 2>&1 > /dev/null
    if [ $? -eq 0 ]; then
    	${DRYRUN} umount -f /dev/xvdb 2>&1 > /dev/null
    	${DRYRUN} sed -i 's/^\/dev\/xvdb.*$//g; /^$/d' /etc/fstab
        echo Unmounted xvdb...
    fi
done

# create fs for each data disks
for ((i = 0; i < ${#IA_DISKS[@]}; i++ ))
do
        d="${IA_DISKS[${i}]}"
        v="/dev/${d}"
        m="/mnt/data$(($i+1))"
        echo Preparing fs on disk ${d}, ${v}...
        ${DRYRUN} mkfs.ext4 -q -t ext4 -F ${v}
        # create mount points
        echo Preparing mount point ${m} for disk ${d}, ${v}...
        if [ ! -d ${m} ]; then
            echo Create mount point ${m} for disk ${d}, ${v}...
            ${DRYRUN} mkdir ${m}
        fi
        ${DRYRUN} chown -R ${IA_USR}.${IA_USR} ${m}
        # mount fs
        ${DRYRUN} mount ${v} ${m}
        ${DRYRUN} chown -R ${IA_USR}.${IA_USR} ${m}
done
echo All disks prepared ready!
