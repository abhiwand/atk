#!/bin/bash

echo "=================================================================================="
echo "Tribeca installation is based on system configuration file /etc/tribeca.cfg"
echo "and user configuration `pwd	`/cfg/tribeca.cfg. "
echo "Please change configuration files if needed."
echo "=================================================================================="
echo " "

configfile_sys="/etc/tribeca.cfg"
configfile_user="`pwd`/../cfg/tribeca.cfg"
INSTALL_DIR=`pwd`

# first read system wide configuration if any
if [ -r $configfile_sys ]; then
    echo "Reading system-wide config:	" $configfile_sys>&2
    cat $configfile_sys
    source $configfile_sys
    echo " "
fi

#if there is user configuration, overwrites system config with user config
if [ -r $configfile_user ]; then
    echo "Reading user config:	" $configfile_user>&2
    cat $configfile_user 
     source $configfile_user
    echo " "
fi

#all the installation packages will be stored at ~/Downloads
if [ ! -d ~/Downloads ]; then
   mkdir ~/Downloads
fi

chmod 777 *.sh

#first make sure there is wget to use
if [ -z `which wget` ] ; then
   echo "Unable to find wget! Cannot proceed with automatic tribeca install."
   exit 1
fi

#install & test hadoop
if [ $INSTALL_HADOOP = "yes" ]; then
   if [ -z `which hadoop` ] ; then
       cd $INSTALL_DIR
       echo "start to install hadoop"

       source install_hadoop.sh $HADOOP_URL $HADOOP_HOME $HADOOP_VERSION $JDK_URL $JDK_VERSION $JDK_VERSION_NUM $HDFS_DIR_PREFIX $HDFS_START_IDX $HDFS_END_IDX

  #only need to test after first installation       
       if [ $TEST_HADOOP = "yes" ]; then
          cd $INSTALL_DIR
          echo "start to test hadoop installation"
          source test_hadoop.sh
       fi
    fi
fi

#install & test hbase
if [ $INSTALL_HBASE = "yes" ]; then
   if [ -z `which hbase` ] ; then
       if [ -z `which hadoop` ] ; then
               echo "Unable to find hadoop! Cannot proceed with automatic hbase install."
               exit 1
       fi
       
       echo "start to install hbase"
       cd $INSTALL_DIR
       if [ ! -d $HBASE_HOME ]; then
          mkdir $HBASE_HOME
       fi
       if [ ! -d $MAVEN_HOME ]; then
          mkdir $MAVEN_HOME
       fi
       source install_hbase.sh  $HBASE_URL $HBASE_HOME $HBASE_VERSION $HADOOP_HOME $HADOOP_VERSION $MAVEN_URL $MAVEN_HOME $MAVEN_VERSION $LAB_MACHINE $MAVEN_REPO_DIR

       if [ $TEST_HBASE = "yes" ]; then
           cd $INSTALL_DIR
           echo "start to test hbase installation"
           source test_hbase.sh $HBASE_HOME $HBASE_VERSION
       fi
    fi
fi


#install & test mongodb
if [ $INSTALL_MONGODB = "yes" ]; then
   if [ -z `which mongodb` ] ; then

       cd $INSTALL_DIR
       echo "start to install mongodb"
       if [ ! -d $MONGODB_HOME ]; then
          mkdir $MONGODB_HOME
       fi

       source install_mongodb.sh  $MONGODB_URL $MONGODB_HOME $MONGODB_VERSION

       if [ $TEST_MONGODB = "yes" ]; then
           cd $INSTALL_DIR
           echo "start to test mongodb installation"
           source test_mongodb.sh $MONGODB_HOME $MONGODB_VERSION
       fi
    fi
fi


#install & test titan
if [ $INSTALL_TITAN = "yes" ]; then
   if [ -z `which hbase` ] ; then
      if [ -z `which cassandra` ] ; then
         echo "Unable to find either hbase or cassandra! Cannot proceed with automatic titan install."
         exit 1
      fi
   fi

       cd $INSTALL_DIR
       echo "start to install titan"
       if [ ! -d $TITAN_HOME ]; then
          mkdir $TITAN_HOME
       fi
       if [ ! -d $MAVEN_HOME ]; then
          mkdir $MAVEN_HOME
       fi
       source install_titan.sh  $TITAN_URL $TITAN_HOME $TITAN_VERSION $MAVEN_URL $MAVEN_HOME $MAVEN_VERSION $USE_INTERNAL $ES_URL $ES_HOME $ES_VERSION $LAB_MACHINE $MAVEN_REPO_DIR

       if [ $TEST_TITAN = "yes" ]; then
          cd $INSTALL_DIR
          echo "start to test titan installation"
          source test_titan.sh $TITAN_HOME
       fi
fi

#install & test faunus
if [ $INSTALL_FAUNUS = "yes" ]; then
       if [ -z `which hadoop` ] ; then
               echo "Unable to find hadoop! Cannot proceed with automatic faunus install."
               exit 1
       fi
       
       cd $INSTALL_DIR
       echo "start to install faunus"
       if [ ! -d $FAUNUS_HOME ]; then
          mkdir $FAUNUS_HOME
       fi
       if [ ! -d $MAVEN_HOME ]; then
          mkdir $MAVEN_HOME
       fi
       source install_faunus.sh  $FAUNUS_URL $FAUNUS_HOME $FAUNUS_VERSION $MAVEN_URL $MAVEN_HOME $MAVEN_VERSION $USE_INTERNAL $LAB_MACHINE $MAVEN_REPO_DIR

       if [ $TEST_FAUNUS = "yes" ]; then
          cd $INSTALL_DIR
          echo "start to test faunus installation"
          source test_faunus.sh $FAUNUS_HOME
       fi
fi

#install & test graphbuilder
if [ $INSTALL_GRAPHBUILDER = "yes" ]; then
       if [ -z `which hadoop` ] ; then
               echo "Unable to find hadoop! Cannot proceed with automatic graphbuilder install."
               exit 1
       fi
       
       cd $INSTALL_DIR
       echo "start to install graphbuilder"
       if [ ! -d $GRAPHBUILDER_HOME ]; then
          mkdir $GRAPHBUILDER_HOME
       fi
       if [ ! -d $MAVEN_HOME ]; then
          mkdir $MAVEN_HOME
       fi
       source install_graphbuilder.sh  $GRAPHBUILDER_URL $GRAPHBUILDER_HOME $GRAPHBUILDER_VERSION $MAVEN_URL $MAVEN_HOME $MAVEN_VERSION $LAB_MACHINE $MAVEN_REPO_DIR

       if [ $TEST_GRAPHBUILDER = "yes" ]; then
          cd $INSTALL_DIR
          echo "start to test graphbuilder installation"
          source test_graphbuilder.sh $GRAPHBUILDER_HOME
       fi
fi

#install & test giraph
if [ $INSTALL_GIRAPH = "yes" ]; then
       if [ -z `which hadoop` ] ; then
               echo "Unable to find hadoop! Cannot proceed with automatic giraph install."
               exit 1
       fi
       
       cd $INSTALL_DIR
       echo "start to install giraph"
       if [ ! -d $GIRAPH_HOME ]; then
          mkdir $GIRAPH_HOME
       fi
       if [ ! -d $MAVEN_HOME ]; then
          mkdir $MAVEN_HOME
       fi
       source install_giraph.sh  $GIRAPH_URL $GIRAPH_HOME $GIRAPH_VERSION $MAVEN_URL $MAVEN_HOME $MAVEN_VERSION $USE_INTERNAL $LAB_MACHINE $MAVEN_REPO_DIR
       
       if [ $TEST_GIRAPH = "yes" ]; then
          cd $INSTALL_DIR
          echo "start to test giraph installation"
          source test_giraph.sh $GIRAPH_HOME $GIRAPH_JAR_VERSION
       fi
fi

#install & test mahout
if [ $INSTALL_MAHOUT = "yes" ]; then
   if [ -z `which mahout` ] ; then
       if [ -z `which hadoop` ] ; then
               echo "Unable to find hadoop! Cannot proceed with automatic mahout install."
               exit 1
       fi

       echo "start to install mahout"
       cd $INSTALL_DIR
       if [ ! -d $MAHOUT_HOME ]; then
          mkdir $MAHOUT_HOME
       fi
       if [ ! -d $MAHOUT_HOME ]; then
          mkdir $MAHOUT_HOME
       fi
       source install_mahout.sh  $MAHOUT_URL $MAHOUT_HOME $MAHOUT_VERSION $MAVEN_URL $MAVEN_HOME $MAVEN_VERSION $USE_INTERNAL $LAB_MACHINE $MAVEN_REPO_DIR

       if [ $TEST_MAHOUT = "yes" ]; then
           cd $INSTALL_DIR
           echo "start to test mahout installation"
           source test_mahout.sh $MAHOUT_HOME
       fi
    fi
fi
                                                                                                                                                                       221,186       99%

#install & test graphlab
if [ $INSTALL_GRAPHLAB = "yes" ]; then
       if [ -z `which hadoop` ] ; then
               echo "Unable to find hadoop! Cannot proceed with automatic graphlab install."
               exit 1
       fi
       
       cd $INSTALL_DIR
       echo "start to install graphlab"
       source install_graphlab.sh $GRAPHLAB_URL $GRAPHLAB_HOME $GRAPHLAB_VERSION $OPENMPI_URL $OPENMPI_VERSION $ANT_URL $ANT_HOME $ANT_VERSION $USE_INTERNAL $GL_SUBNET_ID $GL_SUBNET_MASK $HADOOP_URL $HADOOP_VERSION
       
       if [ $TEST_GRAPHLAB = "yes" ]; then
          cd $INSTALL_DIR
          echo "start to test graphlab installation"
          source test_graphlab.sh $GRAPHLAB_HOME $HADOOP_HOME $HADOOP_VERSION
       fi
fi

