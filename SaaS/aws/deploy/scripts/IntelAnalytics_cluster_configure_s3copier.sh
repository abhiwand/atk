#!/bin/sh
#copy over the s3 copier jar file and upstart script to the master instance. It doesn't necessarily need to run on
#the master but only on one instance that has a public ip to access s3
#if you plan on writing upstart scripts for red hat beware because their is no way to check the syntax

#validate the command line options
TEMP=`getopt -o p:j:c:h:u: --long pem:,jar:,conf:,host:,user: -n 'IntelAnalytics_cluster_configure_s3copier.sh' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

eval set -- "$TEMP"

JARNAME="s3copier.jar"
CONFNAME="s3copier.conf"
COPIERHOME="/var/intelanalytics/s3copier"

while true; do
    case "$1" in
              -p|--pem)
                echo "Option p/pem, argument pem file '$2'"
                PEM=$2
                shift 2 ;;
              -j|--jar)
                echo "Option j/jar, argument jar file: '$2'"
                JAR=$2
                shift 2 ;;
              -c|--conf)
                echo "Option c/conf, upstart script: '$2'"
                CONF=$2
                shift 2 ;;
              -h|--host)
                echo "Option h/host, host: '$2'"
                HOST=$2
                shift 2 ;;
              -u|--user)
                echo "option u/user, user: '$2'"
                USER=$2
                shift 2 ;;
              --) shift ; break ;;
              *) echo "Internal error!" ; exit 1 ;;
    esac
done

#copy over copier jar
scp  -i "$PEM" -p "$JAR"  $USER@$HOST:/home/$USER/$JARNAME
#copy over our upstart conf
scp  -i "$PEM" -p "$CONF"  $USER@$HOST:/home/$USER/$CONFNAME

#stop any current process if any and start s3copier again
ssh -t -i "$PEM" ec2-user@"$HOST" sudo bash -c "'
initctl status s3copier
initctl stop s3copier

mkdir -p $COPIERHOME
mv /home/$USER/$JARNAME $COPIERHOME
mv /home/$USER/$CONFNAME /etc/init/$CONFNAME

initctl reload s3copier
initctl start s3copier
initctl status s3copier
'"