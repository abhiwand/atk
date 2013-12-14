#!/bin/bash
#
#Deploy the desired package to n number of servers with pem files has authentication
#
#ec2-54-200-245-95.us-west-2.compute.amazonaws.com : web1
#ec2-54-200-97-82.us-west-2.compute.amazonaws.com: web2
#ssh -o ProxyCommand='nc -x proxy-socks.jf.intel.com:1080 %h %p' -i IntelAnalytics-SaaS-Admin.pem ec2-user@ec2-54-200-245-95.us-west-2.compute.amazonaws.com
#scp -o ProxyCommand="nc -x  proxy-socks.jf.intel.com:1080 %h %p" -i IntelAnalytics-SaaS-Admin.pem example.sh  ec2-user@ec2-54-200-245-95.us-west-2.compute.amazonaws.com:~
#sudo bin/intelanalytics-web -Dplay.config=prod -Dhttp.port=80 -Dhttps.port=443 -Dhttps.keyStore=conf/\\graphtrial.intel.com.pass.keystore.jks -Dhttps.keyStorePassword=frogsare#0071c5 &

#
echo "arguments"
echo $@
echo ""

TEMP=`getopt -o p:k:t: --long package:,key:,targets: -n 'deploy.bash' -- "$@"`
WEB_DIR="web"
PACKAGE_NAME="intelanalytics-web-1.0-SNAPSHOT"

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

eval set -- "$TEMP"

while true; do
    case "$1" in
              -p|--package)
                echo "Option p/package, argument '$2'"
                PACKAGE=$2
                shift 2 ;;
              -k|--key)
                echo "Option k/key, argument '$2'"
                PEM_FILE=$2
                shift 2 ;;
              -t|--targets)
                echo "Option t/targets, argument '$2'"
                TARGETS=("${TARGETS[@]}" "$2");
                shift 2 ;;
              --) shift ; break ;;
              *) echo "Internal error!" ; exit 1 ;;
    esac
done

startNewPackage="

PID=\$(cat $WEB_DIR/$PACKAGE_NAME/RUNNING_PID)

ls -l $WEB_DIR

echo \"unziping package $WEB_DIR\$PACKAGE_NAME.zip\"
unzip -o $WEB_DIR/$PACKAGE_NAME.zip

if [\$PID == \"\"]; then
    echo \"no current process\"
else

    echo \"killing process \$PID\"
    sudo kill \$PID

    echo \"remove old RUNNING_PID file\"
    rm -rf $WEB_DIR/$PACKAGE_NAME/RUNNING_PID
fi

echo \"Run new process\"
sudo $WEB_DIR/$PACKAGE_NAME/bin/intelanalytics-web -Dplay.config=prod -Dhttp.port=80 -Dhttps.port=443 -Dhttps.keyStore=$WEB_DIR/$PACKAGE_NAME/conf/\\\\graphtrial.intel.com.pass.keystore.jks -Dhttps.keyStorePassword=frogsare#0071c5 &
"

PID= cat RUNNING_PID

echo "$PID"

#ssh to targets and make sure the web dir exist
for t in "${TARGETS[@]}"
    do
    echo "connecting to $t and create web dir"
    ssh -o ProxyCommand='nc -x proxy-socks.jf.intel.com:1080 %h %p' -i "$PEM_FILE" ec2-user@"$t" mkdir web -p
    echo "coping new package over to $t"
    scp -o ProxyCommand="nc -x  proxy-socks.jf.intel.com:1080 %h %p" -i "$PEM_FILE" -p "$PACKAGE"  ec2-user@"$t":~/web/
    ssh -t -t -o ProxyCommand='nc -x proxy-socks.jf.intel.com:1080 %h %p' -i "$PEM_FILE" ec2-user@"$t" <<< "$startNewPackage"
done

echo "Remaining arguments:"
for arg do echo '--> '"\`$arg'" ; done


echo $PACKAGE
echo $PEM_FILE
echo ${TARGETS[@]}

