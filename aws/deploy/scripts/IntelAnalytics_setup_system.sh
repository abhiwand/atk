#!/bin/bash
source IntelAnalytics_env.sh

# set selinux to permissve and disable firewall
for n in `cat ${IA_HOSTS}`
do
	ssh -t -i ${IA_PEM} ${n} "sudo setenforce permissive"
	ssh -t -i ${IA_PEM} ${n} "sudo sed -i 's/=enforcing/=permissive/g' /etc/selinux/config"
	ssh -t -i ${IA_PEM} ${n} "sudo service iptables stop; suod service ip6tables stop;"
	ssh -t -i ${IA_PEM} ${n} "sudo chkconfig iptables off; sudo chkconfig ip6tables off;"
done
