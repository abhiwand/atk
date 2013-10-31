#!/bin/bash
# Notes: executed on the cluster nodes
# This script is not necessarily needed since we are only
# going to mount these data disks once, since no instance
# will ever be rebooted
# -- 
IA_DISKS=(xvdb xvdc xvdd xvde)
IA_USR=hadoop

# create fs for each data disks
for ((i = 0; i < ${#IA_DISKS[${i}]}; i++ ))
do
        d="${IA_DISKS[${i}]}"
        v="/dev/${d}"
        m="/mnt/${d}"
        echo Preparing fstab...
        sudo unmount /dev/xvdb
        sudo sed -i 's/^\/dev\/xvdb.*$//g; /^$/d' /etc/fstab
        echo Preparing fs on disk ${d}, ${v}...
        sudo mkfs.ext4 -q -t ext4 -F ${v}
        # create mount points
        echo Preparing mnt for disk ${d}, ${v}...
        if [ ! -d ${m} ]; then
            sudo mkdir ${m}
        fi
        sudo chown -R ${IA_USR}.${IA_USR} ${m}
        # mount fs
        sudo mount ${v} ${m}
        sudo chown -R ${IA_USR}.${IA_USR} ${m}
done
         
