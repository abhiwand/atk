#!/bin/bash
source IntelAnalytics_env.sh

_script=IntelAnalytics_setup_fstab_node.sh
# note: we know these are the device names upon instance creation
# so don't change them
for n in `cat ${IA_HOSTS}`
do
	scp -i ${IA_PEM} ${_script} ${n}:/tmp/${_script}
	ssh -i ${IA_PEM} -t ${n} "sudo /tmp/${_script}; sudo rm /tmp/${_script}";
done
