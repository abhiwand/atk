#!/bin/bash

HADOOP_URL=$1
HADOOP_HOME=$2
HADOOP_VERSION=$3
JDK_URL=$4
JDK_VERSION=$5
JDK_VERSION_NUM=$6
HDFS_DIR_PREFIX=$7
HDFS_START_IDX=$8
HDFS_END_IDX=$9

CFG_DIR=`pwd`/../cfg
USER_NAME=`whoami`
GROUP_NAME=`id -g -n $USER_NAME`


   echo "Hadoop has not been installed. Start to install hadoop."
   echo "check whether disks are mounted"
#   disk_array=( $(sudo fdisk -l | grep "Disk /dev" | sed 's/://g'| awk '{print $2}') )
#   disk_array=( $(lsscsi | grep -v SSD | awk -F"/" '{print $3}') )
HAS_LSSCSI=`which lsscsi | wc -l`
if [ $HAS_LSSCSI = 0 ]; then
   echo "install lsscsi"
   sudo yum install -y lsscsi
fi

   disk_array=( $(lsscsi | grep -v SSD | awk  '{print $6}') )
   
   #set up hadoop data drives
   #disk_array index start with 0, so max index is ${#disk_array[@]}-1
   let MAX_DISK_IDX=${#disk_array[@]}-1
   if [ $HDFS_END_IDX  -gt $MAX_DISK_IDX ]; then
       END_IDX=$MAX_DISK_IDX
   else
       END_IDX=$HDFS_END_IDX
   fi 

   for (( i=$HDFS_START_IDX; i<=$END_IDX; i++ ))
   do
      formatted=`sudo blkid ${disk_array[$i]} | wc -l`
      DISK=$HDFS_DIR_PREFIX$i
      if [ $formatted = 0  ]; then
         echo "data disk ${disk_array[$i]} is not formatted"
         sudo mkfs.ext4 -F ${disk_array[$i]}
      fi

      if [ ! -d $DISK ]; then
         echo "create $DISK"
         mkdir $DISK 
      fi 

      mounted=`sudo mount | grep ${disk_array[$i]} | wc -l`
      if [ $mounted = 0 ]; then
         echo "mount ${disk_array[$i]} to $DISK"
         sudo mount ${disk_array[$i]} $DISK
         uuid=($(sudo blkid ${disk_array[$i]}  | sed -e 's/^.*UUID="//g' -e 's/" TYPE.*//g'))
         sudo tee -a /etc/fstab > /dev/null <<FIRST
UUID=$uuid   /disk$i   ext4    defaults        0       2
FIRST
      fi
      sudo chown -R $USER_NAME:$GROUP_NAME $DISK
            
          echo "setup hadoop directories at $DISK"
          if [ ! -d $DISK/hadoop-data ]; then
             mkdir $DISK/hadoop-data
          fi
          sudo chmod -R 755 $DISK/hadoop-data
             
          if [ ! -d $DISK/hadoop-mapred ]; then
             mkdir $DISK/hadoop-mapred
          fi
   
          if [ ! -d $DISK/hadoop-name ]; then
                mkdir $DISK/hadoop-name
          fi
             
          if [ $i -eq $HDFS_START_IDX ]; then
             if [ ! -d $DISK/hadoop-tmp ]; then
                mkdir $DISK/hadoop-tmp
             fi
             
             
             sudo chmod -R 777 $DISK/hadoop-tmp
             NAMEDIR="$DISK/hadoop-name"
             TMPDIR="$DISK/hadoop-tmp"
             DATADIR="$DISK/hadoop-data"
             LOCALDIR="$DISK/hadoop-mapred"
          else
             NAMEDIR=$NAMEDIR,"$DISK/hadoop-name"
             DATADIR=$DATADIR,"$DISK/hadoop-data"
             LOCALDIR=$LOCALDIR,"$DISK/hadoop-mapred"
          fi
   done
   
   #install jdk
   if [ -z `which java` ]; then
      echo "install jdk for hadoop"
      cd ~/Downloads
      wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F" $JDK_URL/$JDK_VERSION-b15/jdk-$JDK_VERSION-linux-x64.tar.gz
      #sudo rpm -Uvh jdk-$JDK_VERSION-linux-x64.rpm*
      tar xvfz jdk-$JDK_VERSION-linux-x64.tar.gz*
      if [ ! -d /usr/java ]; then
          sudo mkdir /usr/java
      fi
      sudo mv jdk$JDK_VERSION_NUM  /usr/java/
      sudo alternatives --install /usr/bin/java java /usr/java/latest/bin/java 2000
      #sudo alternatives --auto java
      sudo ln -s /usr/java/jdk$JDK_VERSION_NUM /usr/java/jdk
      #sudo cp /usr/java/jdk$JDK_VERSION_NUM/bin/java* /usr/bin/
      sudo ln -s /usr/java/jdk/bin/javac /usr/bin/javac
      echo "add java to path"
      JAVA_HOME=/usr/java/jdk
      sudo tee -a /etc/profile.d/gaousr.sh > /dev/null <<THIRD
export JAVA_HOME=$JAVA_HOME
export PATH=$PATH:$JAVA_HOME/bin
THIRD
       source /etc/profile
   else
      echo "has java installed already, record JAVA_HOME"
      ALREADY_SET_JAVA_HOME=`env | grep JAVA_HOME | wc -l`
      JAVA_HOME=`which java | sed 's/\/bin\/java/ /g'`
      if [ $ALREADY_SET_JAVA_HOME  -eq 0 ]; then
         sudo tee -a /etc/profile.d/gaousr.sh > /dev/null <<THIRD
export  JAVA_HOME=$JAVA_HOME
export PATH=$PATH:$JAVA_HOME/bin
THIRD
         source /etc/profile
      fi
   fi
   
 
   # echo "install jps for hadoop"
   if [ -z `which jps` ]; then
      sudo yum install -y java-1.7.0-openjdk-devel-1.7.0.25-2.3.10.4.el6_4
   fi
   
   
   #disable ipv6 if it is enabled
   ipv6_disabled=`cat /proc/sys/net/ipv6/conf/all/disable_ipv6`
   
   if [[ $ipv6_disabled == 0 ]]; then
      echo "disable ipv6 in /etc/sysctl.conf" 
      #append /etc/sysctl.conf with the following lines
      sudo tee -a /etc/sysctl.conf > /dev/null <<SECOND
   
      #disable ipv6
      net.ipv6.conf.all.disable_ipv6 = 1
      net.ipv6.conf.default.disable_ipv6 = 1
      net.ipv6.conf.lo.disable_ipv6 = 1
SECOND
   fi
   echo "need to reboot to make the changes in /etc/sysctl.conf take effect"
   
   echo "set up ssh for hadoop"
   if [ ! -f ~/.ssh/id_rsa ]; then
      echo "setup ssh"
      ssh-keygen -b 2048 -t rsa -f ~/.ssh/id_rsa -q -N ""
      cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys 
      chmod 0600 ~/.ssh/authorized_keys 
   fi
   
   ## initial "ssh localhost" needs a input "yes"
   #spawn ssh localhost
   #expect  "Are you sure you want to continue connecting (yes/no)?"
   #send -- " yes\r"


   echo "create $HADOOP_HOME "
   if [ ! -d $HADOOP_HOME ]; then
      mkdir $HADOOP_HOME
   fi

   echo "download hadoop"
   cd ~/Downloads
   wget $HADOOP_URL/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
   tar xvfz hadoop-$HADOOP_VERSION.tar.gz -C $HADOOP_HOME/

   
   echo "update hadoop conf files"
   cd $HADOOP_HOME/hadoop-$HADOOP_VERSION/conf/
   cp $CFG_DIR/hadoop-env.sh .
   cp $CFG_DIR/core-site.xml .
   cp $CFG_DIR/hdfs-site.xml .
   cp $CFG_DIR/mapred-site.xml .
   echo `hostname` > masters
   echo `hostname` > slaves
   
   echo "update JAVA_HOME in hadoop-env.sh"
   #  sed '/# export HADOOP_OPTS=-server/a\export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true"' hadoop-env.sh.bak > hadoop-env.sh
   HADOOP_JAVA_HOME="export JAVA_HOME="$JAVA_HOME
   sed -i '9d' hadoop-env.sh
   sed -i "9i$HADOOP_JAVA_HOME" hadoop-env.sh
   
   
   
   echo "update core-site.xml"
   HOST_IP=`ifconfig | grep inet  | sed 's/:/ /g' | awk '{print $3}' | grep -v 127.0.0.1 | grep -v 192.168 | head -n 1`
   DEFAULTNAME=" <value>hdfs://"`hostname`":19010</value>"
   sed -i '10d' core-site.xml
   sed -i "10i$DEFAULTNAME" core-site.xml
   
   TMPDIR1=" <value>$TMPDIR</value>"
   sed -i '15d' core-site.xml
   sed -i "15i$TMPDIR1" core-site.xml
   
   echo "update conf/hdfs-site.xml"
   NAMEDIR1="   <value>$NAMEDIR</value>"
   sed -i '5d' hdfs-site.xml
   sed -i "5i$NAMEDIR1" hdfs-site.xml
   
   DATADIR1=" <value>$DATADIR</value>"
   sed -i '10d' hdfs-site.xml
   sed -i "10i$DATADIR1" hdfs-site.xml
   
   echo "update conf/mapred-site.xml"
   JOBTRACKER="   <value>"`hostname`":19011</value>"
   sed -i '5d' mapred-site.xml
   sed -i "5i$JOBTRACKER" mapred-site.xml
   
   LOCALDIR1="   <value>$LOCALDIR</value>"
   sed -i '10d' mapred-site.xml
   sed -i "10i$LOCALDIR1" mapred-site.xml
   
   echo "format name node"
   $HADOOP_HOME/hadoop-$HADOOP_VERSION/bin/hadoop namenode -format -force
   
   echo "add hadoop to path"
   sudo tee -a /etc/profile.d/gaousr.sh > /dev/null <<THIRD
export  PATH=$PATH:$HADOOP_HOME/hadoop-$HADOOP_VERSION/bin
THIRD
   
   source /etc/profile



   echo "Set up hadoop for all users in this grop to use."
   echo "start hadoop"
   start-all.sh
   jps
   hadoop dfsadmin -report
   hadoop dfsadmin -safemode leave

   if hadoop fs -test -d $TMPDIR/mapred/staging; then
   else
         echo "mkdir $TMPDIR/mapred/staging"
         hadoop fs -mkdir $TMPDIR/mapred/staging
   fi
   hadoop fs -chmod -R 777 $TMPDIR/mapred/staging
   echo "chmod 777 $TMPDIR"
   sudo chmod 777 $TMPDIR
   # everyone can write to /tmp in hdfs
   hadoop fs -mkdir /tmp
   hadoop fs -chmod -R 777 /tmp

   
   cd $TMPDIR
   echo "the following users are in the same group as me"
   USER_ARRAY=( $(sudo lid -g -n $GROUP_NAME | grep -v $USER_NAME) )

   for (( i=0; i<${#USER_ARRAY[@]}; i++ ))
   do
        echo "set up hadoop for  ${USER_ARRAY[$i]}"
        echo "mkdir /user/${USER_ARRAY[$i]}"
        hadoop fs -mkdir /user/${USER_ARRAY[$i]}
        echo "change owner  of /user/${USER_ARRAY[$i]} to  ${USER_ARRAY[$i]}"
        hadoop fs -chown -R ${USER_ARRAY[$i]}:supergroup /user/${USER_ARRAY[$i]}
        #everyone owns his/her staging
        hadoop fs -mkdir $TMPDIR/mapred/staging/${USER_ARRAY[$i]}
        hadoop fs -chown -R ${USER_ARRAY[$i]}:supergroup  $TMPDIR/mapred/staging/${USER_ARRAY[$i]}
        hadoop fs -chmod -R 700 $TMPDIR/mapred/staging/${USER_ARRAY[$i]}
   done
