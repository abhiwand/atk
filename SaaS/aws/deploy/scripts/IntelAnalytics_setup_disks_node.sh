#!/bin/bash
# Notes: executed on the cluster nodes in sudo
# This script is not necessarily needed since we are only
# going to mount these data disks once, since no instance
# will ever be rebooted
# -- 
IA_DISKS=(xvdb xvdc xvdd xvde)
IA_USR=hadoop
# the AMI default mounts xvdb
# create fs for each data disks
echo Start making fs on disk ${IA_DISKS[@]}
for ((i = 0; i < ${#IA_DISKS[@]}; i++ ))
do
    mount | grep xvdb 2>&1 > /dev/null
    if [ $? -eq 0 ]; then
    	umount -f /dev/xvdb 2>&1 > /dev/null
    	sed -i 's/^\/dev\/xvdb.*$//g; /^$/d' /etc/fstab
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
        mkfs.ext4 -q -t ext4 -F ${v} &
        pids[${i}]=$!
        echo Making file system on disk ${d}, ${v} in process ${pids[${i}]}...
done
echo Waiting for mkfs to finish on processes ${pids[@]}...
wait ${pids[@]}
echo Preparing mount points for fs on ${IA_DISKS[@]}...
for ((i = 0; i < ${#IA_DISKS[@]}; i++ ))
do
        # create mount points
        d="${IA_DISKS[${i}]}"
        v="/dev/${d}"
        m="/mnt/data$(($i+1))"
        echo Preparing mount point ${m} for disk ${d}, ${v}...
        if [ ! -d ${m} ]; then
            echo Create mount point ${m} for disk ${d}, ${v}...
            mkdir ${m}
        fi
        chown -R ${IA_USR}.${IA_USR} ${m}
        # mount fs
        mount ${v} ${m}
        chown -R ${IA_USR}.${IA_USR} ${m}
done
