#!/bin/bash

HADOOP_URL=$1
HADOOP_HOME=$2
HADOOP_VERSION=$3
JDK_URL=$4
JDK_VERSION=$5
JDK_VERSION_NUM=$6
START_DISK=$7
SINGLE_DISK=$8

CONF_DIR=`pwd`/../cfg

   echo "Hadoop has not been installed. Start to install hadoop."
   echo "check whether disks are mounted"
   disk_array=( $(sudo fdisk -l | grep "Disk /dev" | sed 's/://g'| awk '{print $2}') )
   
   #first disk is for boot
   #second disk as working directory
 if [ $SINGLE_DISK = "yes"  ]; then
 echo "for single disk, create directories for hadoop"
          if [ ! -d /data1]; then
             sudo mkdir /data1
          fi
     
          DISK="/data1"
          sudo chown -R `whoami`:`whoami` $DISK
          if [ ! -d $DISK/hadoop-data ]; then
             mkdir $DISK/hadoop-data
          fi
          sudo chmod -R 755 $DISK/hadoop-data
             
          if [ ! -d $DISK/hadoop-mapred ]; then
             mkdir $DISK/hadoop-mapred
          fi
   
             if [ ! -d $DISK/hadoop-tmp ]; then
                mkdir $DISK/hadoop-tmp
             fi
             
             if [ ! -d $DISK/hadoop-name ]; then
                mkdir $DISK/hadoop-name
             fi
             
             sudo chmod -R 777 $DISK/hadoop-tmp
             NAMEDIR="$DISK/hadoop-name"
             TMPDIR="$DISK/hadoop-tmp"
             DATADIR="$DISK/hadoop-data"
             LOCALDIR="$DISK/hadoop-mapred"

 else
   for (( i=$START_DISK; i<${#disk_array[@]}; i++ ))
   do
   mounted=`grep -w ${disk_array[$i]} /etc/fstab |wc -l`
      if [ $mounted = 0  ]; then
         echo "data disk is not mounted"
         sudo mkfs.ext4 ${disk_array[$i]}
         sudo mkdir /disk$i
         sudo mount ${disk_array[$i]}  /disk$i
         sudo tee -a /etc/fstab > /dev/null <<FIRST
   ${disk_array[$i]}		/disk$i	ext4	defaults	0	2          
FIRST
   
   
          sudo chown -R `whoami`:`whoami` /disk$i
   
          if [ ! -d /disk$i/hadoop-data ]; then
             mkdir /disk$i/hadoop-data
          fi
          sudo chmod -R 755 /disk$i/hadoop-data
             
          if [ ! -d /disk$i/hadoop-mapred ]; then
             mkdir /disk$i/hadoop-mapred
          fi
   
          if [ ! -d /disk$i/hadoop-name ]; then
                mkdir /disk$i/hadoop-name
          fi
             
          if [ $i = $START_DISK ]; then
             if [ ! -d /disk$i/hadoop-tmp ]; then
                mkdir /disk$i/hadoop-tmp
             fi
             
   
            sudo chmod -R 777 /disk$i/hadoop-tmp
             NAMEDIR="/disk$i/hadoop-name"
             TMPDIR="/disk$i/hadoop-tmp"
             DATADIR="/disk$i/hadoop-data"
             LOCALDIR="/disk$i/hadoop-mapred"
          else
             NAMEDIR=$NAMEDIR,"/disk$i/hadoop-name"
             DATADIR=$DATADIR,"/disk$i/hadoop-data"
             LOCALDIR=$LOCALDIR,"/disk$i/hadoop-mapred"
          fi
          
      else
          echo "data disk already mounted"
          DISK=`grep -w ${disk_array[$i]} /etc/fstab | awk '{print $2}' `
          sudo chown -R `whoami`:`whoami` $DISK
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
             
          if [ $i = $START_DISK ]; then
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
      fi
   done
 
 fi
   
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
      sudo cp /usr/java/jdk$JDK_VERSION_NUM/bin/java* /usr/bin/
      echo "add java to path"
      JAVA_HOME=/usr/java/jdk
      sudo tee -a /etc/bashrc > /dev/null <<THIRD
          export PATH=$PATH:$JAVA_HOME/bin
THIRD
       source /etc/bashrc
   #    sudo chown -R root:root $JAVA_HOME/bin 
   else
      echo "has java installed already, record JAVA_HOME"
      ALREADY_SET_JAVA_HOME=`env | grep JAVA_HOME | wc -l`
      JAVA_HOME=`which java | sed 's/\/bin\/java/ /g'`
      if [ $ALREADY_SET_JAVA_HOME  -eq 0 ]; then
         sudo tee -a /etc/bashrc > /dev/null <<THIRD
             export  JAVA_HOME=$JAVA_HOME
THIRD
         source /etc/bashrc
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


   echo "download hadoop"
   cd ~/Downloads
   wget $HADOOP_URL/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
   tar xvfz hadoop-$HADOOP_VERSION.tar.gz -C $HADOOP_HOME/

   
   echo "update hadoop conf files"
   cd $HADOOP_HOME/hadoop-$HADOOP_VERSION/conf/
   cp $CONF_DIR/hadoop-env.sh .
   cp $CONF_DIR/core-site.xml .
   cp $CONF_DIR/hdfs-site.xml .
   cp $CONF_DIR/mapred-site.xml .
   
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
   sudo tee -a /etc/bashrc > /dev/null <<THIRD
          export  PATH=$PATH:$HADOOP_HOME/hadoop-$HADOOP_VERSION/bin
THIRD
   
   source /etc/bashrc



   echo "Set up hadoop for all users in this grop to use."
   echo "start hadoop"
   start-all.sh
   jps
   hadoop dfsadmin -report
   hadoop dfsadmin -safemode leave

   HAS_DIR=`hadoop fs -ls $TMPDIR/mapred/staging | wc -l`
   if [ $HAS_DIR -eq 0  ]; then
      hadoop fs -mkdir $TMPDIR/mapred/staging
   fi
   hadoop fs -chmod -R 777 $TMPDIR/mapred/staging
   sudo chmod 777 $TMPDIR
   
   cd $TMPDIR
   USER_NAME=`whoami`
   GROUP_NAME=`id -g -n $USER_NAME`
  ### USER_ARRAY=`sudo lid -g  $GROUP_NAME | cut -f1 -d'(' | grep -v $USER_NAME`
   echo "the following users are in the same group as me"
   USER_ARRAY=( $(sudo lid -g -n $GROUP_NAME | grep -v $USER_NAME) )

   for (( i=0; i<${#USER_ARRAY[@]}; i++ ))
   do
        echo "set up hadoop for  ${USER_ARRAY[$i]}"
  #     sudo mkdir  ${USER_ARRAY[$i]}
  #     sudo chown -R ${USER_ARRAY[$i]}:$GROUP_NAME ${USER_ARRAY[$i]}/
        echo "mkdir /user/${USER_ARRAY[$i]}"
        hadoop dfs -mkdir /user/${USER_ARRAY[$i]}
        echo "change owner  of /user/${USER_ARRAY[$i]} to  ${USER_ARRAY[$i]}"
        hadoop dfs -chown -R ${USER_ARRAY[$i]}:supergroup /user/${USER_ARRAY[$i]}
   done
