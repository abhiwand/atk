rexdir=`cd $(dirname $0)/.. > /dev/null; pwd`
logdir=$rexdir/logs
cfgdir=$rexdir/conf
logbase=rexstitan-`hostname`-`date +%Y-%m-%d`
loglog=${logdir}/${logbase}.log
logout=${logdir}/${logbase}.out
log4j=${cfgdir}/log4j-rexstitan.properties
rexcfg=${cfgdir}/rexstitan-hbase-es.xml
pid=/var/run/hadoop/pids/rexstitan-${USER}-rexster.pid
name="Titan Rexster Server"

# Find Java
if [ "${JAVA_HOME}" = "" ] ; then
     echo "JAVA_HOME is not set!"
     exit 1
fi
