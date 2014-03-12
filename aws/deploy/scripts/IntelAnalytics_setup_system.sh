#!/bin/bash
# Description: Used for do system configuration on the cluster for hadoop
# Note: Expected to be executed from admin node
source IntelAnalytics_setup_env.sh

# set selinux to permissve and disable firewall
for n in `cat ${IA_HOSTS}`
do
	ssh -t -i ${IA_PEM} ${n} "sudo setenforce permissive"
	ssh -t -i ${IA_PEM} ${n} "sudo sed -i 's/=enforcing/=permissive/g' /etc/selinux/config"
	ssh -t -i ${IA_PEM} ${n} "sudo service iptables stop; suod service ip6tables stop;"
	ssh -t -i ${IA_PEM} ${n} "sudo chkconfig iptables off; sudo chkconfig ip6tables off;"
done
