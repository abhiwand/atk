#!/bin/bash
source IntelAnalytics_env.sh

for n in `cat ${IA_HOSTS}`
do
	ssh -t -i ${IA_PEM} ${n} "sudo groupadd -g ${IA_UID};"
	ssh -t -i ${IA_PEM} ${n} "sudo useradd -g ${IA_UID} -u ${IA_UID} -s /bin/bash -m -d /home/${IA_USR} ${IA_USR};"
	ssh -t -i ${IA_PEM} ${n} "echo ${IA_USR} | sudo passwd --stdin ${IA_USR};"
	scp -i ${IA_PEM} -r ${IA_USRSSH} ${n}:/home/${IA_USR}
	ssh -t -i ${IA_PEM} ${n} "sudo chown -R ${IA_USR}.${IA_USR} /home/${IA_USR}"
	ssh -t -i ${IA_PEM} ${n} 'sudo chmod 700 /home/${IA_USR}/.ssh'
done
