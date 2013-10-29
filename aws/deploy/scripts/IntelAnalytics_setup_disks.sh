#!/bin/bash
source IntelAnalytics_env.sh

# TODO: this is hard-coded right now, but should come from input file
DISKS=(xvdb xvdc xvdd xvde)

# create fs for each data disks
for n in `cat ${IA_HOSTS}`
do
	for ((i = 0; i < ${#DISKS[${i}]}; i++ ))
	do
		d="${DISKS[${i}]}"
		v="/dev/${d}"
		m="/mnt/${d}"
		echo Preparing disk ${d} (${v}) on ${n}...
		ssh -i ${IA_PEM} -t ${n} "sudo mkfs.ext4 -q -t ext4 -F ${v}"
		
		# create mount points
		echo Preparing mount points ${m} for ${d}(${v}) on ${n}...
		ssh -i ${IA_PEM} -t ${n} "if [ ! -d ${m} ]; then sudo mkdir ${m}; sudo chown -R ${IA_USR}.${IA_USR} ${m}; fi"
	done
done
