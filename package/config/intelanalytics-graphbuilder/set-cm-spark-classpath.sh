#!/bin/bash
#Will update the spark_env config value in cloudera manager for the cluster this host is running in
#The host must be a valid cloudera cluster and it needs to run spark.
#All request are through the cloudera managers rest api version 6

INTERACTIVE=true
API_VERSION="v6"
API_PREFIX="api"
API_BASE_URL="${API_PREFIX}/${API_VERSION}"

SPARK_SERVICE_NAME="SPARK"
SPARK_CLOUDERA_CONFIG_GROUP="spark-GATEWAY-BASE"
SPARK_ENV_CONFIG_NAME="spark-conf/spark-env.sh_client_config_safety_valve"
INTEL_ANALYTICS_SPARK_CLASSPATH="/usr/lib/intelanalytics/graphbuilder/lib/"
green=$(tput setaf 2)
red=$(tput setaf 1)
yellow=$(tput setaf 3)
normal=$(tput sgr0)

CM_PORT=7180
CM_HOST=$(cat /etc/cloudera-scm-agent/config.ini | grep server_host | awk -F"=" '{print $2}')
USERNAME="admin"
PASSWORD="admin"
CLUSTERNAME=""
RESTART=false
while true; do
    case "$1" in
		--interactive)
			echo "interactive, $2"
			INTERACTIVE=$2
			shift 2 ;;
		--host)
			echo "host, '$2'"
			CM_HOST=$2
			shift 2 ;;
		--port)
			echo "port, '$2'"
			CM_PORT=$2
			shift 2 ;;
		--username)
			echo "username, '$2'"
			USERNAME=$2
			shift 2 ;;
		--password)
			echo "password, '$2'"
 			PASSWORD=$2
			shift 2 ;;
		--clustername)
			echo "clustername, '$2'"
			CLUSTERNAME=$2
			shift 2 ;;
		--restart)
			echo "restart, '$2'"
			RESTART=$2
			shift 2 ;;
		--) shift ; break ;;
		*) break ;;
    esac
done

reader=""
function readIn(){
	reader=""
	if [ "$INTERACTIVE" == "true" ]; then
		read $1 reader 
	fi
}


echo "What is the cloudera manager's host address? will default to ${CM_HOST}:"
readIn
if [ "$reader" != "" ]; then
	CM_HOST=$reader
fi

if [ ! -d /etc/cloudera-scm-agent/config.ini ] && [ "$CM_HOST" == "" ]; then
    echo "${red}No cloudera manager host given or found${normal}"
    exit 1
fi

echo "What is the cloudera manager port? will default to 7180:"
readIn
if [ "$reader" != "" ]; then
	CM_PORT=$reader
fi

echo "What is the cloudera manager user? will default to admin:"
readIn
if [ "$reader" != "" ]; then
	USERNAME=$reader
fi

echo "What is the cloudera manager password? will default to admin:"
readIn -s
if [ "$reader" != "" ]; then
        PASSWORD=$reader
fi

HOST=$(cat /etc/cloudera-scm-agent/config.ini | grep server_host | awk -F"=" '{print $2}')
if [ "$HOST" == "" ]; then
    echo "This script is not running on a cloudera cluster please specify the cluster's name as it appears in the cloudera manager?"
    read CLUSTERNAME
fi


echo "connecting to ${USERNAME}:${PASSWORD} ${CM_HOST}:${CM_PORT}"
CM_URL="${CM_HOST}:7180"
curlLoginOpt="-u${USERNAME}:${PASSWORD}"

url=""
#Will create a base url with server:port/base api post fix
#the first parameter should bhe the path to the api you are calling
#minus the SERVER:PORT/api/v#
function createUrl(){
	setUrl ${CM_URL}/${API_BASE_URL}/$1
}
function setUrl(){
	url=$1
}

deployRoles=""
function getDeployConfigRoles(){
	createUrl clusters/${clusterNameEncoded}/services/${sparkService}/roles
	roles=$(curl -s  ${curlLoginOpt} ${url} | jq -c -r '.items[].name')
	 deployRoles='{"items":['
        for role in $roles
        do
                deployRoles=$deployRoles'"'${role}'",'
        done
        deployRoles=${deployRoles%,}
        deployRoles=${deployRoles}']}'
}
function deployConfig(){
	getDeployConfigRoles
	createUrl clusters/${clusterNameEncoded}/services/${sparkService}/commands/deployClientConfig
	#doCurl POST ${deployRoles} 1
    response=$(curl -s -H "Content-Type: application/json" -X POST -d${deployRoles}  ${curlLoginOpt} ${url} | jq -c -r '{id,active}')
    deployConfigCommandId=$(echo $response| jq '.id' )
    activeStatus=$(echo $response| jq '.active' )

    if [ "$deployConfigCommandId" == "" ] && [ "$activeStatus" != "true" ]; then
        echo "${red}couldn't deploy configuration${normal}"
        exit 1
    else
        echo -n "${yellow}Deploying Config${normal}"
    fi

    createUrl clusters/${clusterNameEncoded}/services/${sparkService}/commands
    count=1
    while [ $count -ne 0 ]
    do
        count=0
        for commandId in `curl -s -H "Content-Type: application/json" -X GET  ${curlLoginOpt} ${url} | jq -c -r '.items[].id' `
        do
            if [ "$commandId" == "$deployConfigCommandId" ]; then
                count=$(($count +1))
                echo -n "."
	        	sleep 3
		break
            fi
        done
    done
    if [ $count -eq 0 ];then
        echo "${green}config deployed to spark master and workers${normal}"
        restartSpark
    fi

}

function restartSpark(){
    echo "The spark service must be restarted for the changes to take affect."
    echo "If you want to restart spark service type 'yes'"
    readIn
    if [ "$reader" != "" ]; then
    	RESTART=$reader
    fi

    if [ "$RESTART" == "yes" ]; then

        createUrl clusters/${clusterNameEncoded}/services/${sparkService}/commands/restart
        response=$(curl -s -H "Content-Type: application/json" -X POST  ${curlLoginOpt} ${url} | jq -c -r '{id,active}')
	    restartCommandId=$(echo $response | jq -c -r '.id')
	    activeStatus=$(echo $response| jq '.active' )

	    if [ "$restartCommandId" == "" ] && [ "$activeStatus" != "true" ]; then
            echo "${red}couldn't restart spark service${normal}"
            exit 1
        else
            echo -n "${yellow}restarting spark${normal}"
        fi

        createUrl clusters/${clusterNameEncoded}/services/${sparkService}/commands
        count=1
        while [ $count -ne 0 ]
        do
            count=0
            for commandId in `curl -s -H "Content-Type: application/json" -X GET  ${curlLoginOpt} ${url} | jq -c -r '.items[].id' `
            do
                if [ "$commandId" == "$restartCommandId" ]; then
                    count=$((count +1))
                    echo -n "."
		            sleep 3
                    break
                fi
            done
        done
        if [ $count -eq 0 ];then
            echo "${green}Spark restarted${normal}"
        fi
    fi
    return 0
}

function setUpdatedClassPath(){
        local existingClassPath=$1
        existingClassPath=${existingClassPath% }
		existingClassPath=${existingClassPath# }
        if [ "$existingClassPath" == "" ]; then
                updatedClassPath="${INTEL_ANALYTICS_SPARK_CLASSPATH}"
        else
                updatedClassPath="${existingClassPath}:${INTEL_ANALYTICS_SPARK_CLASSPATH}"
        fi
}

function doCurl(){
	local verb=$1
	local json=$2
	local debug=$3

	http_code=$(curl -s -o /dev/null -w "%{http_code}" -H "Content-Type: application/json" -X$verb -d$json  ${curlLoginOpt} ${url}) #--trace-ascii /dev/stdout)

	if [ "$http_code" != "200" ]; then
		echo "$red failed to set rest call $normal"
		exit 1
	fi
}

function setConfig(){
	local json=$1
	createUrl clusters/${clusterNameEncoded}/services/spark/roleConfigGroups/spark-GATEWAY-BASE/config
	doCurl PUT  '{"items":[{"name":"spark-conf/spark-env.sh_client_config_safety_valve","value":"'$json'"}]}' 1
    deployConfig
}

IFS=$'\n'

#store the url encoded cluster name
clusterNameEncoded=""
#human readable clustername
clusterName=""

if [ "$CLUSTERNAME" == "" ]; then
    createUrl "hosts?view=full"
    for host in `curl -s ${curlLoginOpt} ${url} | jq -c -r '.items[] | {hostId,ipAddress,hostname,roleRefs}' `
    do
        hostName=$(echo -E $host | jq -c -r '.hostname')
        myHostName=$(hostname -f)
        if [ "$hostName" == "$myHostName" ]; then
            echo "${green}found host${normal}"
            for roleRef in `echo -E $host | jq -c -r '.roleRefs[]'`
            do
                serviceName=$(echo -E $roleRef | jq -c -r '.serviceName')
                clusterName=$(echo -E $roleRef | jq -c -r '.clusterName')

                if [ "$serviceName" == "spark" ]; then
                    echo "${green}Found cluster owner${normal}"
                    clusterNameEncoded=$(perl -MURI::Escape -e 'print uri_escape($ARGV[0]);' "${clusterName}")
                    break
                fi
            done
        fi
        if [ "$clusterNameEncoded" != "" ]; then
            break;
        fi
    done
    if [ "$clusterNameEncoded" == "" ]; then
        echo "${red}Couldn't find this host's cluster${normal}"
        exit 1
    fi
else
    clusterNameEncoded=$(perl -MURI::Escape -e 'print uri_escape($ARGV[0]);' "${CLUSTERNAME}")
    clustername=$clusterNameEncoded
fi

#we technically did do a service check before when were were trying to match the host id to a cluster but 
#i would rather query the services directly to find out if it's running rather than indirectly through the hosts url
echo This host belongs to cluster: $clusterName
echo Verifing cluster is running spark
createUrl clusters/${clusterNameEncoded}/services
sparkService=""
for service in `curl -s ${curlLoginOpt} $url | jq -c -r '.items[] | {name,type,serviceState}'`
do 
	name=$(echo $service | jq -c -r '.name')
	type=$(echo $service | jq -c -r '.type')
	serviceState=$(echo $service | jq -c -r '.serviceState')
	if [ "$type" == "$SPARK_SERVICE_NAME" ]; then
		sparkService=$name
		if [ "$serviceState" != "STARTED" ]; then
                	echo "$yellow Spark service is not running $normal"
        	fi
		break
	fi
done

if [ "$sparkService" == "" ]; then
	echo "${red} Cluster \'${$clusterName}\' is not running spark${normal}"
	exit 1
fi

#make sure the service is installed an running on the cluster
if [ "$sparkService" != "" ]; then
	echo "Setting/updating the SPARK_CLASSPATH for cluster: $clusterName"
	createUrl clusters/${clusterNameEncoded}/services/${sparkService}/roleConfigGroups/${SPARK_CLOUDERA_CONFIG_GROUP}/config
	configSet=0
	for configItem in `curl -s ${curlLoginOpt} ${url} | jq -c -r '.items[] | {name,value}'`
	do
		configName=$(echo  $configItem | jq -c -r '.name')
		configValue=$(echo $configItem | jq -c '.value')
		#i have to not due raw input for value or i loose my carrige returns
		#since i don't do raw input i have to strip quotes
		configValue=${configValue%\"}
		configValue=${configValue#\"}
		if [ "$configName" == "$SPARK_ENV_CONFIG_NAME" ]; then
			configSet=1	
			break
		fi
	done

	if [ $configSet -eq 1 ]; then
		echo Updating cloudera spark env

		existingClassPathExport=$(echo  $configValue | grep "SPARK_CLASSPATH=")
		exitingClassPath=""

		if [ "$existingClassPathExport" != "" ]; then
			#They have a classpath that i need to modify

			#read the config and only keep the spark classpath i don't care about anything else			
			echo -E $configValue > /tmp/spark_env.sh.tmp				
			existingClassPath=$(sed "s/.*\(SPARK_CLASSPATH=\(\\\\\".*\\\\\"\|[^\\\r\\\n]*\)\).*/\1/" /tmp/spark_env.sh.tmp | awk -F"=" '{print $2}')

			#remove quotes
			existingClassPath=${existingClassPath%\\\"}
			existingClassPath=${existingClassPath#\\\"}
			#look for existing intel analytic classpaths
			existingIntelAnalyticsClassPath=$(echo $existingClassPath | grep ${INTEL_ANALYTICS_SPARK_CLASSPATH%\*})
			
			if [ "$existingIntelAnalyticsClassPath" == "" ]; then
				echo No current intel analytics SPARK_CLASSPATH entry, appending $INTEL_ANALYTICS_SPARK_CLASSPATH to SPARK_CLASSPATH
				setUpdatedClassPath $existingClassPath
				
				echo -E $configValue > /tmp/spark_env.sh.tmp
        	                sed -i "s/export SPARK_CLASSPATH=\(\\\\\".*\\\\\"\|[^\\\r\\\n]*\)/export SPARK_CLASSPATH=\\\\\"REPLACEME\\\\\"/g"  /tmp/spark_env.sh.tmp

                       		sed -i  "s|REPLACEME|$updatedClassPath\*|Ig"  /tmp/spark_env.sh.tmp

                	        configValue=$(cat /tmp/spark_env.sh.tmp)
				
				setConfig $configValue
				echo "${green}SPARK_CLASSPATH updated and deployed ${normal}"
				exit 0
			else
				echo  "${green}Existing intel analytics spark classpath entry no changes needed. Current spark classpath: ${existingClassPath} ${normal}"
			fi
		else
			echo "Setting SPARK_CLASSPATH none set"
			setUpdatedClassPath ""
			configValue=$configValue'\r\n export SPARK_CLASSPATH=\"'$updatedClassPath\*'\"'
			
			setConfig $configValue
			echo "${green}SPARK_CLASSPATH updated and deployed ${normal}"
			exit 0
		fi
	else
		echo Setting cloudera spark env
		
		setUpdatedClassPath ""
                echo $updatedClassPath
                configValue='export SPARK_CLASSPATH=\"'$updatedClassPath\*'\"'

                setConfig $configValue
                echo "${green}SPARK_CLASSPATH updated and deployed ${normal}"
                exit 0
	fi
else
	echo "${red}Spark is not runing on the cluster no classpath to set/update, exitting ${normal}"
	exit 1
fi




