#!/bin/bash

configValue=$(python ${CONF_DIR}/scripts/config.py --host $ATK_CDH_HOST --port $ATK_CDH_PORT --username $ATK_CDH_USERNAME --password $ATK_CDH_PASSWORD --service "SPARK" --config-group "spark-SPARK_WORKER-BASE" --config "SPARK_WORKER_role_env_safety_valve")
#configValue=$(python config.py --host 10.54.8.175 --password wolverine --service "SPARK" --config-group "spark-SPARK_WORKER-BASE" --config "SPARK_WORKER_role_env_safety_valve" --value "SPARK_CLASSPATH=\"/usr/lib/intelanalytics/graphbuilder/lib/ispark-deps.jar\"" --set yes --restart yes)
echo $configValue

INTEL_ANALYTICS_SPARK_CLASSPATH="${ATK_SPARK_DEPS_DIR}/${ATK_SPARK_DEPS_JAR}"
ATK_PROCESS_FILE=${ATK_TEMP}/spark_env.sh.tmp
#INTEL_ANALYTICS_SPARK_CLASSPATH="/usr/lib/intelanalytics/graphbuilder/lib/ispark-deps.jar"

function setConfig(){
    python ${CONF_DIR}/scripts/config.py --host $ATK_CDH_HOST --port $ATK_CDH_PORT --username $ATK_CDH_USERNAME --password $ATK_CDH_PASSWORD --service "SPARK" --config-group "spark-SPARK_WORKER-BASE" --config "SPARK_WORKER_role_env_safety_valve" --value "$1" --set yes --restart yes
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

    if [ "$configValue" != "" ]; then
		echo Updating cloudera spark env

		existingClassPathExport=$(echo  $configValue | grep "SPARK_CLASSPATH=")
		exitingClassPath=""

		if [ "$existingClassPathExport" != "" ]; then
			#They have a classpath that i need to modify

			#read the config and only keep the spark classpath i don't care about anything else
			echo -E $configValue > $ATK_PROCESS_FILE
			existingClassPath=$(sed "s/.*\(SPARK_CLASSPATH=\(\\\".*\\\"\|[^\\r\\n]*\)\).*/\1/" $ATK_PROCESS_FILE | awk -F"=" '{print $2}')

			#remove quotes
			existingClassPath=${existingClassPath%\\\"}
			existingClassPath=${existingClassPath#\\\"}
			#look for existing intel analytic classpaths
			existingIntelAnalyticsClassPath=$(echo $existingClassPath | grep ${INTEL_ANALYTICS_SPARK_CLASSPATH%\*})

			if [ "$existingIntelAnalyticsClassPath" == "" ]; then
				echo No current intel analytics SPARK_CLASSPATH entry, appending $INTEL_ANALYTICS_SPARK_CLASSPATH to SPARK_CLASSPATH
				setUpdatedClassPath $existingClassPath

				echo -E $configValue > $ATK_PROCESS_FILE
                sed -i "s/SPARK_CLASSPATH=\(\\\".*\\\"\|[^\\r\\n]*\)/SPARK_CLASSPATH=\"REPLACEME\"/g"  $ATK_PROCESS_FILE

                sed -i  "s|REPLACEME|$updatedClassPath|Ig"  $ATK_PROCESS_FILE

                configValue=$(cat $ATK_PROCESS_FILE)

				setConfig $configValue
				echo "${green}SPARK_CLASSPATH updated and deployed ${normal}"
				exit 0
			else
				echo  "${green}Existing intel analytics spark classpath entry no changes needed. Current spark classpath: ${existingClassPath} ${normal}"
			fi
		else
			echo "Setting SPARK_CLASSPATH none set"
			setUpdatedClassPath ""
			configValue=$configValue'\r\n SPARK_CLASSPATH="'$updatedClassPath'"'

			setConfig $configValue
			echo "${green}SPARK_CLASSPATH updated and deployed ${normal}"
			exit 0
		fi
	else
		echo Setting cloudera spark env

		setUpdatedClassPath ""
        echo $updatedClassPath
        configValue='SPARK_CLASSPATH="'$updatedClassPath'"'

        setConfig $configValue
        echo "SPARK_CLASSPATH updated"
        exit 0
	fi
