=========================
|IA| Package Installation
=========================

.. contents::
    :local:
    
------------
Introduction
------------

In this guide we will walk through the |IA| installation and the minimal
configuration needed to get the service running.
This guide is not going to walk you through the Cloudera cluster installation
since that subject is covered by Cloudera in greater detail.

See `Cloudera Installation Documentation
<http://www.cloudera.com/content/cloudera-content/cloudera-docs/CM5/latest/Cloudera-Manager-Installation-Guide/cm5ig_install_cm_cdh.html>`__.

------------
Requirements
------------

Operating System Requirements
=============================

These instructions will work on `Red Hat Enterprise Linux
<http://redhat.com/>`__ or `CentOS <http://centos.org/>`__ version 6.6

User Permission Requirements 
============================

Since you will be installing packages via the 'yum' command, you will need
'sudo' command access to execute 'yum' commands successfully.
To verify access open a terminal session and run this quick command::

    sudo yum repolist

If you don't have permissions to run the 'sudo' command you will see access
denied errors.

::

    [sudo] password for jdoe:
    jdoe is not in the sudoers file. This incident will be reported.

If you don't have permissions to run 'sudo' contact your system administrator.

If you do have access to 'sudo' and 'yum' you will see a list of repositories:

.. only:: html
    
    ::

        Loaded plugins: amazon-id, rhui-lb, s3
        repo id                                   repo name                                                     status
        epel                                      Extra Packages for Enterprise Linux 6 - x86_64                11,099
        rhui-REGION-client-config-server-6        Red Hat Update Infrastructure 2.0 Client Configuration Server      3
        rhui-REGION-rhel-server-releases          Red Hat Enterprise Linux Server 6 (RPMs)                      12,888
        rhui-REGION-rhel-server-releases-optional Red Hat Enterprise Linux Server 6 Optional (RPMs)              7,269
        rhui-REGION-rhel-server-rh-common         Red Hat Enterprise Linux Server 6 RH Common (RPMs)                27
        repolist: 31,335

.. only:: latex
    
    ::

        Loaded plugins: amazon-id, rhui-lb, s3
        repo id                          repo name
        epel                             Extra Packages for Enterprise Linux 6 - x...
        rhui-REGION-client-config-ser... Red Hat Update Infrastructure 2.0 Client ...
        rhui-REGION-rhel-server-relea... Red Hat Enterprise Linux Server 6 (RPMs) ...
        rhui-REGION-rhel-server-relea... Red Hat Enterprise Linux Server 6 Optiona...
        rhui-REGION-rhel-server-rh-co... Red Hat Enterprise Linux Server 6 RH Comm...
        repolist: 31,335

Cluster Requirements
====================

You need a Cloudera cluster 5.3.x with following services.

i.  HDFS
#.  SPARK
#.  Hbase
#.  Yarn(MR2)
#.  Zookeeper

You need Python to run the |IA| Python client.
The |IA| Python client will run with Python 2.7.

Yum Repository Requirements
===========================

All the nodes on the cluster must have the EPEL yum repository as well as two
|IA| repositories.
You will need repository access to |IA| Private Repository which you will get
when you sign up.

If you have trouble downloading any of the dependencies run::

    yum clean all

or::

    yum clean expire-cache
 
---------------------------------
|IA| Packages Support Information
---------------------------------

See :doc:`ad_inst_ia2`

--------------------------
|IA| Packages Installation
--------------------------

Adding Extra Repositories
=========================

The first step in the installation is adding EPEL and two |IA| repositories to
make the `YUM <http://en.wikipedia.org/wiki/Yellowdog_Updater,_Modified>`__
installation possible.
The EPEL and |IA| repositories must be installed on all spark master and
worker nodes as well as the node that will be running the |IA| rest server.
The |IA| Dependency repository and the yum-s3 package must be installed before
trying to add the |IA| :ref:`private repository <add_IA_private_repository>`.

Add EPEL Repository
-------------------

Before trying to install the EPEL repo run the following command to see if
it's already available on the machines you will be installing |IA| on.

::

    sudo yum repolist

.. only:: html

    Sample output::

        repo id                                    repo name
        cloudera-cdh5                              Cloudera Hadoop, Version 5                                           141
        cloudera-manager                           Cloudera Manager, Version 5.3.1                                        7
        epel                                       Extra Packages for Enterprise Linux 6 - x86_64                    11,022
        rhui-REGION-client-config-server-6         Red Hat Update Infrastructure 2.0 Client Configuration Server 6        2
        rhui-REGION-rhel-server-releases           Red Hat Enterprise Linux Server 6 (RPMs)                          12,690
        rhui-REGION-rhel-server-releases-optional  Red Hat Enterprise Linux Server 6 Optional (RPMs)                  7,168

.. only:: latex

    Sample output::

        repo id                           repo name
        cloudera-cdh5                     Cloudera Hadoop, Version 5            ...
        cloudera-manager                  Cloudera Manager, Version 5.3.1       ...
        epel                              Extra Packages for Enterprise Linux 6 ...
        rhui-REGION-client-config-ser...  Red Hat Update Infrastructure 2.0 Clie...
        rhui-REGION-rhel-server-relea...  Red Hat Enterprise Linux Server 6 (RPM...
        rhui-REGION-rhel-server-relea...  Red Hat Enterprise Linux Server 6 Opti...

You want to look for ``epel`` repo id.

If the EPEL repository is missing run these commands to install the necessary
files:

.. only:: html

    ::

        wget http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
        sudo rpm -ivh epel-release-6-8.noarch.rpm

.. only:: latex

    ::

        wget http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.
            noarch.rpm
        sudo rpm -ivh epel-release-6-8.noarch.rpm

To verify the installation run::

    sudo yum repolist
 
.. only:: html

    Sample output::

        repo id                                    repo name
        epel                                       Extra Packages for Enterprise Linux 6 - x86_64                   11,018
        rhui-REGION-client-config-server-6         Red Hat Update Infrastructure 2.0 Client Configuration Server 6       2
        rhui-REGION-rhel-server-releases           Red Hat Enterprise Linux Server 6 (RPMs)                         12,663
        rhui-REGION-rhel-server-releases-optional

.. only:: latex

    Sample output::

        repo id                           repo name
        epel                              Extra Packages for Enterprise Linux 6...
        rhui-REGION-client-config-ser...  Red Hat Update Infrastructure 2.0 Cli...
        rhui-REGION-rhel-server-relea...  Red Hat Enterprise Linux Server 6 (RP...
        rhui-REGION-rhel-server-relea...  Red Hat Enterprise Linux Server 6 Opt...
                                           
Make sure the "epel" repo id is present.



Add |IA| Dependency Repository
------------------------------

We pre-package and host some open source libraries to aid with installations.
In some cases we pre-package newer versions from what is available in RHEL,
EPEL or CentOS repositories.

.. only:: html

    To add the dependency repository run the following command::

        wget https://intel-analytics-dependencies.s3-us-west-2.amazonaws.com/ia-deps.repo
        sudo cp ia-deps.repo /etc/yum.repos.d/

.. only:: latex

    To add the dependency repository run the following command::

        wget https://intel-analytics-dependencies.s3-us-west-2.amazonaws.com/
            ia-deps.repo
        sudo cp ia-deps.repo /etc/yum.repos.d/

If you have issues running the above command, try entering the following,
being careful about the placement of the ``"`` characters::

    echo "[intel-analytics-deps]
    name=intel-analytics-deps
    baseurl=https://intel-analytics-dependencies.s3-us-west-2.amazonaws.com/yum
    gpgcheck=0
    priority=1 enabled=1"  | sudo tee -a /etc/yum.repos.d/ia-deps.repo

To test the installation of the dependencies repository run the following
command::

    sudo yum info yum-s3

    #should print something close to this
    Available Packages
    Name        : yum-s3
    Arch        : noarch
    Version     : 0.2.4
    Release     : 1
    Size        : 9.0 k
    Repo        : intel-analytics-deps
    Summary     : Amazon S3 plugin for yum.
    URL         : git@github.com:NumberFour/yum-s3-plugin.git
    License     : Apache License 2.0

If you get similar output, install yum-s3 package::

    sudo yum -y install yum-s3

Installing the YUM s3 plugin will allow us to use the Amazon S3 repository.

.. _add_IA_private_repository:

Add |IA| Private Repository
---------------------------

Next we will create /etc/yum.repos.d/ia.repo.
Don't forget to replace ``YOUR_KEY``, and ``YOUR_SECRET`` with your given AWS
access, and secret keys.

Run the following command to create ``/etc/yum.repos.d/ia.repo`` file.

.. only:: html

    ::

        echo "[intel-analytics]
        name=intel-analytics
        baseurl=https://intel-analytics-repo.s3-us-west-2.amazonaws.com/release/latest/yum/dists/rhel/6
        gpgcheck=0
        priority=1
        s3_enabled=1
        #yum-get iam only has get
        key_id=YOUR_KEY
        secret_key=YOUR_SECRET" | sudo tee -a /etc/yum.repos.d/ia.repo

    Other baseurls to try if having difficulty::

        baseurl=https://s3-us-west-2.amazonaws.com/intel-analytics-repo/release/#.#.#/yum/dists/rhel/6 
        baseurl=https://s3-us-west-2.amazonaws.com/intel-analytics-repo/release/latest/yum/dists/rhel/6

.. only:: latex

    ::

        echo "[intel-analytics]
        name=intel analytics
        baseurl=https://intel-analytics-repo.s3-us-west-2.amazonaws.com
            /release/latest/yum/dists/rhel/6
        gpgcheck=0
        priority=1
        s3_enabled=1
        #yum-get iam only has get
        key_id=YOUR_KEY
        secret_key=YOUR_SECRET" | sudo tee -a /etc/yum.repos.d/ia.repo

    Other baseurls to try if having difficulty::

        baseurl=https://s3-us-west-2.amazonaws.com/intel-analytics-repo
            /release/#.#.#/yum/dists/rhel/6 
        baseurl=https://s3-us-west-2.amazonaws.com/intel-analytics-repo
            /release/latest/yum/dists/rhel/6

    Note: baseurl lines above are broken for readability.
    They should be entered as a single line.

.. Note::

    Don't forget to replace **YOUR_KEY**, and **YOUR_SECRET** with the keys
    that were given to you.

Verify the installation of the |IA| repository by running::

    sudo yum info intelanalytics-rest-server

    #sample output
    Available Packages
    Name        : intelanalytics-rest-server
    Arch        : x86_64
    Version     : #.#.#
    Release     : ####
    Size        : 419 M
    Repo        : intel-analytics
    Summary     : intelanalytics-rest-server-0.9
    URL         : intel.com
    License     : Confidential

If you get package details for intelanalytics-rest-server package,
then the repository installed correctly and you can continue installation.

Troubleshooting Private Repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The most common error when using the private repository is in correct access
and secret keys or the server time is out of sync with the world.
It never hurts to double check your access and secret keys in the ia.repo file.

AWS S3 will fail with access denied errors if the system time is out of sync.
To keep your system time in sync with the world run::

    sudo service ntpd start


The |IA| Dependency repository and the yum-s3 package must be installed before
trying to add the |IA| :ref:`private repository <add_IA_private_repository>`.

If you are performing the yum commands inside a corporate proxy make sure your
http_proxy and https_proxy environment variables are set.
When you run the sudo command you may need to add the -E option to keep
environment variables from your current session.

::

    $ sudo -E yum command

.. _installing_IA_packages:

Installing |IA| Packages
========================

Installing Master Node
----------------------

This next step is going to install the |IA| REST server and all it's
dependencies.
Only one instance of the rest server needs to be installed.
Although it doesn't matter where it's installed it's usually installed along
side the HDFS name node.

::

    sudo yum -y install intelanalytics-rest-server

Installing Worker Node
----------------------

The Intel Analytics spark dependencies package needs to be installed on every
node running the spark worker role.

.. only:: html

    ::

        $ sudo yum -y install intelanalytics-spark-deps intelanalytics-python-rest-client

.. only:: latex

    ::

        $ sudo yum -y install intelanalytics-spark-deps
            intelanalytics-python-rest-client

**[Troubleshooting Tip]**

.. only:: html

    Error message::
        
        https://...rpm: [Errno 14] PYCURL ERROR 22 - "The requested URL returned error: 403 Forbidden"

.. only:: latex

    Error message::
        
        https://...rpm: [Errno 14] PYCURL ERROR 22 - 
        "The requested URL returned error: 403 Forbidden"

If you ever get 403 Forbidden error, 99% of the time it is because a new
branch was just built and replaced the old file.
Usually doing a ``yum clean expire-cache`` will fix your issue.

-------------------------
REST Server Configuration
-------------------------

/etc/intelanalytics/rest-server/application.conf
================================================

We will be creating the application.conf file by copying and renaming the file
application.conf.tpl file in the same directory.
We have a configuration script that will create the application.conf for you.
We also walk through the manual configuration, if you would like to get
familiar with the |IA| configuration.

.. note::

  Before you update the application.conf file, first you need to create a new
  database and user in postgresql.
  You will do all the database and user creation commands from the postgres
  client.
  See the section on `postgresql <ia_inst_ia1_postgresql>`_.

Configuration Script
--------------------

The configuration of application.conf is semi-automated via the use of a
Python script in /etc/intelanalytics/rest-server/config.py.
It will query Cloudera Manager for the necessary configuration values and
create a new application.conf based off the application.conf.tpl file.
The script will also fully configure your local PostgreSQL installation to
work with the |IA| server.

To configure your spark service and your |IA| installation do the following::

    cd /etc/intelanalytics/rest-server/
    sudo ./config

After executing the script, answer the prompts to configure your cluster.

.. only:: html

    Sample output with notes::

        #if the default is correct hit enter
        $ sudo ./config

        What port is Cloudera Manager listening on? defaults to "7180" if nothing is entered:
        What is the Cloudera Manager username? defaults to "admin" if nothing is entered:
        What is the Cloudera Manager password? defaults to "admin" if nothing is entered:
     
        No current SPARK_CLASSPATH set.
        Setting to:
        SPARK_CLASSPATH="/usr/lib/intelanalytics/graphbuilder/lib/ispark-deps.jar"

        Deploying config   .   .   .   .   .   .   .   .   .   .   .   .  
        Config Deployed

        You need to restart Spark service for the config changes to take affect.
        Would you like to restart now? Enter 'yes' to restart. defaults to 'no' if nothing is
        entered: yes
        Restarting Spark  .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   
        Restarted Spark


        What is the hostname of the database server? defaults to "localhost" if nothing is entered:
        What is the port of the database server? defaults to "5432" if nothing is entered:
        What is the name of the database? defaults to "ia_metastore" if nothing is entered:
        What is the database user name? defaults to "iauser" if nothing is entered:
         
        #The dollar sign($) is not allowed in the password. 
        What is the database password? The default password was randomly generated.
            Defaults to "****************************" if nothing is entered:

        Creating application.conf file from application.conf.tpl
        Reading application.conf.tpl
        Updating configuration
        Configuration created for Intel Analytics
        Configuring postgres access for  "iauser"
        Initializing database:                                     [  OK  ]
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        CREATE ROLE
        0
        CREATE DATABASE
        0
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        initctl: Unknown instance:
        intelanalytics-rest-server start/running, process 17484
        0
        Waiting for Intel Analytics server to restart
         .   .   .   .
        You are now connected to database "ia_metastore".
        INSERT 0 1
        0
        postgres is configured
        Intel Analytics is ready for use.

.. only:: latex

    Sample output with notes::

        #if the default is correct hit enter
        $ sudo ./config

        What port is Cloudera Manager listening on?
            defaults to "7180" if nothing is entered:
        What is the Cloudera Manager username?
            defaults to "admin" if nothing is entered:
        What is the Cloudera Manager password?
            defaults to "admin" if nothing is entered:
     
        No current SPARK_CLASSPATH set.
        Setting to:
        SPARK_CLASSPATH="/usr/lib/intelanalytics/graphbuilder/lib/ispark-deps.jar"

        Deploying config   .   .   .   .   .   .   .   .   .   .   .   .  
        Config Deployed

        You need to restart Spark service for the config changes to take affect.
        Would you like to restart now? Enter 'yes' to restart. defaults to 'no' if nothing is
        entered: yes
        Restarting Spark  .   .   .   .   .   .   .   .   .   .   .   .   .   .   .
        Restarted Spark


        What is the hostname of the database server?
            defaults to "localhost" if nothing is entered:
        What is the port of the database server?
            defaults to "5432" if nothing is entered:
        What is the name of the database?
            defaults to "ia_metastore" if nothing is entered:
        What is the database user name?
            defaults to "iauser" if nothing is entered:
         
        #The dollar sign($) is not allowed in the password. 
        What is the database password?
            The default password was randomly generated.
            Defaults to "****************************" if nothing is entered.

        Creating application.conf file from application.conf.tpl
        Reading application.conf.tpl
        Updating configuration
        Configuration created for Intel Analytics
        Configuring postgres access for  "iauser"
        Initializing database:                                     [  OK  ]
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        CREATE ROLE
        0
        CREATE DATABASE
        0
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        initctl: Unknown instance:
        intelanalytics-rest-server start/running, process 17484
        0
        Waiting for Intel Analytics server to restart
         .   .   .   .
        You are now connected to database "ia_metastore".
        INSERT 0 1
        0
        postgres is configured
        Intel Analytics is ready for use.

The script will walk you through all the necessary configurations to get the
|IA| service running.
You can run the script multiple times but be very careful when configuring the
database multiple times because you can wipe out a users data frames and
graphs. 

Command line arguments can also be supplied for every single prompt.
If a command line argument is given no prompt will ever be presented.
To get a list of all the command line arguments for the configuration script
run the same command with --help::

    $ sudo ./config --help

    usage: config [-h] [--host HOST] [--port PORT] [--username USERNAME]
                  [--password PASSWORD] [--cluster CLUSTER] [--python PYTHON]
                  [--restart RESTART] [--db-host DB_HOST] [--db-port DB_PORT]
                  [--db DB] [--db-username DB_USERNAME]
                  [--db-password DB_PASSWORD]
                  [--db-skip-reconfig DB_SKIP_RECONFIG]

    Process cl arguments to avoid prompts in automation optional arguments:
        -h, --help            show this help message and exit
        --host HOST           Cloudera Manager Host
        --port PORT           Cloudera Manager Port
        --username USERNAME   Cloudera Manager User Name
        --password PASSWORD   Cloudera Manager Password
        --cluster CLUSTER     Cloudera Manager Cluster Name if more than one cluster
                              is managed by Cloudera Manager.
        --python PYTHON       The name of the Python executable to use. It must be
                              in the path
        --restart RESTART     Weather or not to restart spark service after config
                              changes
        --db-host DB_HOST     Database host name
        --db-port DB_PORT     Database port number
        --db DB               Database name
        --db-username DB_USERNAME
                              Database username
        --db-password DB_PASSWORD
                              Database password
        --db-skip-reconfig DB_SKIP_RECONFIG
                              Should i skip database re-configuration? 'yes' to
                              skip.

Manual Configuration
--------------------
**This section is optional and only if additional changes to the configuration
file are needed.** (`Skip section`_ )
 
The rest-server package only provides a configuration template called
application.conf.tpl.
We need to copy and rename this file to application.conf and update host names
and memory configurations.
First let's copy and rename the file:

.. only:: html

    ::

        sudo cp /etc/intelanalytics/rest-server/application.conf.tpl /etc/intelanalytics/rest-server/application.conf

.. only:: latex

    ::

        sudo cp /etc/intelanalytics/rest-server/application.conf.tpl
            /etc/intelanalytics/rest-server/application.conf

    Note:
    The above command has been split for enhanced readability in some medias.
    It should be entered as a single line.

Open the file with your editor of choice::

    sudo vi /etc/intelanalytics/rest-server/application.conf

All the changes that need to be made are at the top of the file.
This is the section you want to look at::

    # BEGIN REQUIRED SETTINGS

    intel.analytics {
    #bind address - change to 0.0.0.0 to listen on all interfaces
    //api.host = "127.0.0.1"

    #bind port
    //api.port = 9099

    # The host name for the Postgresql database in which the metadata will be stored
    metastore.connection-postgresql.host = "localhost"
    metastore.connection-postgresql.port = "5432"
    metastore.connection-postgresql.database = "ia_metastore"
    metastore.connection-postgresql.username = "iauser"
    metastore.connection-postgresql.password = "MyPassword"
    metastore.connection-postgresql.url = "jdbc:postgresql://"${intel.analytics.metastore.connection-postgresql.host}":"${intel.analytics.metastore.connection-postgresql.port}"/"${intel.analytics.metastore.connection-postgresql.database}

    # This allows for the use of postgres for a metastore. Service restarts will not affect the data stored in postgres
    metastore.connection = ${intel.analytics.metastore.connection-postgresql}

    # This allows the use of an in memory data store. Restarting the rest server will create a fresh database and any
    # data in the h2 DB will be lost
    //metastore.connection = ${intel.analytics.metastore.connection-h2}

    engine {

        # The hdfs URL where the intelanalytics folder will be created
        # and which will be used as the starting point for any relative URLs
        fs.root = "hdfs://master.silvern.gao.cluster:8020/user/iauser"

        # The (comma separated, no spaces) Zookeeper hosts that
        # Comma separated list of host names with zookeeper role assigned
        titan.load.storage.hostname = "node01, node02, node01"

        # Titan storage backend. Available options are hbase and cassandra. The default is hbase
        //titan.load.storage.backend = "hbase"

        # Titan storage port, defaults to 2181 for HBase ZooKeeper. Use 9160 for Cassandra
        titan.load.storage.port = "2181"

        # The URL for connecting to the Spark master server
        #spark.master = "spark://master.silvern.gao.cluster:7077"
        yarn-client = "spark://master.silvern.gao.cluster:7077"


        spark.conf.properties {
            # Memory should be same or lower than what is listed as available in Cloudera Manager.
            # Values should generally be in gigabytes, e.g. "64g"
            spark.executor.memory = "103079215104"
        }
    }
    
    }
    # END REQUIRED SETTINGS

    # The settings below are all optional. Some may need to be configured depending on the
    # specifics of your cluster and workload.

    intel.analytics {
      engine-spark {
        auto-partitioner {
          # auto-partitioning spark based on the file size
          file-size-to-partition-size = [{ upper-bound="1MB", partitions = 15 },
                                           { upper-bound="1GB", partitions = 45 },
                                           { upper-bound="5GB", partitions = 100 },
                                           { upper-bound="10GB", partitions = 200 },
                                           { upper-bound="15GB", partitions = 375 },
                                           { upper-bound="25GB", partitions = 500 },
                                           { upper-bound="50GB", partitions = 750 },
                                           { upper-bound="100GB", partitions = 1000 },
                                           { upper-bound="200GB", partitions = 1500 },
                                           { upper-bound="300GB", partitions = 2000 },
                                           { upper-bound="400GB", partitions = 2500 },
                                           { upper-bound="600GB", partitions = 3750 }]
      # max-partitions is used if value is above the max upper-bound
              max-partitions = 10000
          }
        }
      
        # Configuration for the Intel Analytics REST API server
        api {
          #this is reported by the API server in the /info results - it can be used to identify
          #a particular server or cluster
          //identifier = "ia"

          #The default page size for result pagination
          //default-count = 20
          
          #Timeout for waiting for results from the engine
          //default-timeout = 30s
          
          #HTTP request timeout for the api server
          //request-timeout = 29s
        }
        
          #Configuration for the IAT processing engine
          engine {
              //default-timeout = 30s
             //page-size = 1000
             
        spark {
        
          # When master is empty the system defaults to spark://`hostname`:7070 where hostname is calculated from the current system
          # For local mode (useful only for development testing) set master = "local[4]"
          # in cluster mode, set master and home like the example
          # master = "spark://MASTER_HOSTNAME:7077"
          # home = "/opt/cloudera/parcels/CDH/lib/spark"
          
          # When home is empty the system will check expected locations on the local system and use the first one it finds
          # ("/usr/lib/spark","/opt/cloudera/parcels/CDH/lib/spark/", etc)
          //home = ""
          
          conf {
            properties {
              # These key/value pairs will be parsed dynamically and provided to SparkConf()
              # See Spark docs for possible values http://spark.apache.org/docs/0.9.0/configuration.html
              # All values should be convertible to Strings
              
              #Examples of other useful properties to edit for performance tuning:
              
              # Increased Akka frame size from default of 10MB to 100MB to allow tasks to send large results to Spark driver
              # (e.g., using collect() on large datasets)
              //spark.akka.frameSize=100
              
              #spark.akka.retry.wait=30000
              #spark.akka.timeout=200
              #spark.akka.timeout=30000
              
              //spark.shuffle.consolidateFiles=true
              
              # Enabling RDD compression to save space (might increase CPU cycles)
              # Snappy compression is more efficient
              //spark.rdd.compress=true
              //spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec
              
              #spark.storage.blockManagerHeartBeatMs=300000
              #spark.storage.blockManagerSlaveTimeoutMs=300000
              
              #spark.worker.timeout=600
              #spark.worker.timeout=30000
              spark.eventLog.enabled=true
              spark.eventLog.dir="hdfs://master.silvern.gao.cluster:8020/user/spark/applicationHistory"
            }
            
          }
        }
        
        giraph {
          #Overrides of normal Hadoop settings that are used when running Giraph jobs
          giraph.maxWorkers = 30
          //giraph.minWorkers = 1
          //giraph.SplitMasterWorker = true
          mapreduce.map.memory.mb = 8192
          mapreduce.map.java.opts = "-Xmx6144m"
          //giraph.zkIsExternal = false
        }
        
        
        titan {
          load {
            # documentation for these settings is available on Titan website
            # http://s3.thinkaurelius.com/docs/titan/current/titan-config-ref.html
            storage {
            
              # Whether to enable batch loading into the storage backend. Set to true for bulk loads.
              //batch-loading = true
              
              # Size of the batch in which mutations are persisted
              //buffer-size = 2048
              
              lock {
                #Number of milliseconds the system waits for a lock application to be acknowledged by the storage backend
                //wait-time = 400
                
                #Number of times the system attempts to acquire a lock before giving up and throwing an exception
                //retries = 15
              }
              
              hbase {
                # Pre-split settngs for large datasets
                //region-count = 12
                //compression-algorithm = "SNAPPY"
              }
              
              cassandra {
                # Cassandra configuration options
              }
            }
            
            ids {
              #Globally reserve graph element IDs in chunks of this size. Setting this too low will make commits
              #frequently block on slow reservation requests. Setting it too high will result in IDs wasted when a
              #graph instance shuts down with reserved but mostly-unused blocks.
              //block-size = 300000
              
              #Number of partition block to allocate for placement of vertices
              //num-partitions = 10
              
              #The number of milliseconds that the Titan id pool manager will wait before giving up on allocating a new block of ids
              //renew-timeout = 150000
              
              #When true, vertices and edges are assigned IDs immediately upon creation. When false, IDs are assigned
              #only when the transaction commits. Must be disabled for graph partitioning to work.
              //flush = true
              
              authority {
                #This setting helps separate Titan instances sharing a single graph storage
                #backend avoid contention when reserving ID blocks, increasing overall throughput.
                # The options available are:
                #NONE = Default in Titan
                #LOCAL_MANUAL = Expert feature: user manually assigns each Titan instance a unique conflict avoidance tag in its local graph configuration
                #GLOBAL_MANUAL = User assigns a tag to each Titan instance. The tags should be globally unique for optimal performance,
                #                but duplicates will not compromise correctness
                #GLOBAL_AUTO = Titan randomly selects a tag from the space of all possible tags when performing allocations.
                //conflict-avoidance-mode = "GLOBAL_AUTO"
                
                #The number of milliseconds the system waits for an ID block reservation to be acknowledged by the storage backend
                //wait-time = 300
                
                # Number of times the system attempts ID block reservations with random conflict avoidance tags
                # before giving up and throwing an exception
                //randomized-conflict-avoidance-retries = 10
              }
            }
            
            auto-partitioner {
              hbase {
                # Number of regions per regionserver to set when creating Titan/HBase table
                regions-per-server = 2
                
                # Number of input splits for Titan reader is based on number of available cores
                # and minimum split size as follows: Number of splits = Minimum(input-splits-per-spark-core * spark-cores,
                #     graph size in HBase/minimum-input-splits-size-mb)
                input-splits-per-spark-core = 20
              }
              
              enable = true
            }
          }
          
          query {
            storage {
              # query does use the batch load settings in titan.load
              backend = ${intel.analytics.engine.titan.load.storage.backend}
              hostname =  ${intel.analytics.engine.titan.load.storage.hostname}
              port =  ${intel.analytics.engine.titan.load.storage.port}
            }
            cache {
              # Adjust cache size parameters if you experience OutOfMemory errors during Titan queries
              # Either increase heap allocation for IntelAnalytics Engine, or reduce db-cache-size
              # Reducing db-cache will result in cache misses and increased reads from disk
              //db-cache = true
              //db-cache-clean-wait = 20
              //db-cache-time = 180000
              #Allocates 30% of available heap to Titan (default is 50%)
              //db-cache-size = 0.3
            }
          }
        }
      }
    }

.. _ad_inst_IA_configure_file_system_root:

Configure File System Root
~~~~~~~~~~~~~~~~~~~~~~~~~~

In the following line the text "invalid-fsroot-host" should be replaced with
the fully qualified domain of your HDFS Namenode::

    fs.root = "hdfs://invalid-fsroot-host/user/iauser"

Example::

    fs.root = "hdfs://localhost.localdomain/user/iauser" 

If your HDFS Name Node port does not use the standard port, you can specify it
after the host name with a colon::

    fs.root = "hdfs://localhost.localdomain:8020/user/iauser"

Configure Zookeeper Hosts
~~~~~~~~~~~~~~~~~~~~~~~~~

In the following line replace "invalid-titan-host" with the comma delimited
list of fully qualified domain names of all nodes running the zookeeper
service::

    titan.load.storage.hostname = "invalid-titan-host"

Example::

    titan.load.storage.hostname = "localhost.localdomain" 

If your zookeeper client port is not 2181 un-comment the following line and
replace 2181 with your zookeeper client port::

    titan.load.storage.port = "2181"

Configure Spark Master Host
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Update "invalid-spark-master" with the fully qualified domain name of the
Spark master node::

    spark.master = "spark://invalid-spark-master:7077"

Example::

    spark.master = "spark://localhost.localdomain:7077" 

Configure Spark Executor Memory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Spark executor memory needs to be set equal to or less than what is
configured in Cloudera Manager.
The Cloudera Spark installation will, by default, set the Spark executor
memory to 8g, so 8g is usually a safe setting.
If have any doubts you can always verify the executor memory in Cloudera
Manager.

::

    spark.executor.memory = "invalid executor memory"

Example::

    spark.executor.memory = "8g"

Click on the Spark service then configuration in Cloudera Manager to get
executor memory.

.. image:: ad_inst_ia_01.*
    :align: center

Set the Bind IP Address (Optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you would like the |IA| server to bind to all ip addresses and not just
localhost update the following lines and follow the commented instructions.
This configuration section is also near the top of the file.

::

    #bind address - change to 0.0.0.0 to listen on all interfaces
    //host = "127.0.0.1"

Updating the Spark Class Path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you did the automatic configuration the classpath will be updated
automatically in Cloudera Manager but if you have any problems you can update
the spark class path through Cloudera Manager.
If you log into Cloudera Manager under the spark configuration you can find
the Worker Environment Advanced Configuration Snippet.
If it isn't already set, add::

    SPARK_CLASSPATH="/usr/lib/intelanalytics/graphbuilder/lib/ispark-deps.jar"

.. image:: ad_inst_ia_02.*
    :align: center

.. _Skip section:

**End of manual configuration**

Now, restart the Spark service.

.. image:: ad_inst_ia_03.*
    :align: center

Database Configuration
----------------------

The Intel Analytics service can use two different databases H2 and PostgreSQL.
If you run the auto configuration script postgresql is fully configured for
you.

**H2**

Configuration is very simple but you lose all your metadata on a service
restart.
Enabling H2 is very easy and only requires us to comment and uncomment some
lines in the application.conf file.
To comment a line in the configuration file either prepend the line with two
forward slashes '//' or a pound sign '#'.

The following lines need to be commented:

.. only:: html

    *Before* ::

        metastore.connection-postgresql.host = "invalid-postgresql-host"
        metastore.connection-postgresql.port = 5432
        metastore.connection-postgresql.database = "ia-metastore"
        metastore.connection-postgresql.username = "iauser"
        metastore.connection-postgresql.password = "myPassword"
        metastore.connection-postgresql.url = "jdbc:postgresql://"${intel.analytics.metastore.connection-postgresql.host}":"${intel.analytics.metastore.connection-postgresql.port}"/"${intel.analytics.metastore.connection-postgresql.database}
        metastore.connection = ${intel.analytics.metastore.connection-postgresql}
    
    *After* ::

        //metastore.connection-postgresql.host = "invalid-postgresql-host"
        //metastore.connection-postgresql.port = 5432
        //metastore.connection-postgresql.database = "ia-metastore"
        //metastore.connection-postgresql.username = "iauser"
        //metastore.connection-postgresql.password = "myPassword"
        //metastore.connection-postgresql.url = "jdbc:postgresql://"${intel.analytics.metastore.connection-postgresql.host}":"${intel.analytics.metastore.connection-postgresql.port}"/"${intel.analytics.metastore.connection-postgresql.database}
        //metastore.connection = ${intel.analytics.metastore.connection-postgresql}

.. only:: latex

    *Before* ::

        metastore.connection-postgresql.host = "invalid-postgresql-host"
        metastore.connection-postgresql.port = 5432
        metastore.connection-postgresql.database = "ia-metastore"
        metastore.connection-postgresql.username = "iauser"
        metastore.connection-postgresql.password = "myPassword"
        metastore.connection-postgresql.url = "jdbc:postgresql://"${intel.analytics.
            metastore.connection-postgresql.host}":"${intel.analytics.metastore.
            connection-postgresql.port}"/"${intel.analytics.metastore.connection-
            postgresql.database}
        metastore.connection = ${intel.analytics.metastore.connection-postgresql}
    
    *After* ::

        //metastore.connection-postgresql.host = "invalid-postgresql-host"
        //metastore.connection-postgresql.port = 5432
        //metastore.connection-postgresql.database = "ia-metastore"
        //metastore.connection-postgresql.username = "iauser"
        //metastore.connection-postgresql.password = "myPassword"
        //metastore.connection-postgresql.url = "jdbc:postgresql://"${intel.analytics.
            metastore.connection-postgresql.host}":"${intel.analytics.metastore.
            connection-postgresql.port}"/"${intel.analytics.metastore.connection-
            postgresql.database}
        //metastore.connection = ${intel.analytics.metastore.connection-postgresql}

Next uncomment the following line:

*Before* ::

    //metastore.connection = ${intel.analytics.metastore.connection-h2}

*After* ::

    metastore.connection = ${intel.analytics.metastore.connection-h2}

.. _ad_inst_ia1_postgresql:

**Postgresql**

The PostgreSQL configuration is a bit more advanced and should probably only be
attempted by an advanced user.
Using PostgreSQL will allow your graphs and frames to persist across service
restarts.

First, log into the postges user on the linux system::

    $ sudo su postgres

Start the postgres command line client::

    $ psql

Wait for the command line prompt to come::

    postgres=# 

Then create a user::

    postgres=# create user YOURUSER with createdb encrypted password 'YOUR_PASSWORD';

User creation confirmation::

    CREATE ROLE

Then create a database for that user::

    postgres=# create database YOURDATABASE with owner YOURUSER;

Database creation confirmation::

    CREATE DATABASE

After creating the database exit the postgres command line by hitting ``ctrl + d``

Once your database and user are created, open '/var/lib/pgsql/data/pg_hba.conf'
and add this line
``host    all         YOURUSER     127.0.0.1/32            md5``
to the top of the file::

    $ vi /var/lib/pgsql/data/pg_hba.conf

You can add the new line at the very top of the file or before any uncommented
lines.
If the pg_hba.conf file doesn't exist you need to initialize postgresql with::

    $ sudo survice postgresql initdb
 
Now that you created your database, you can enter the configuration in the
``application.conf`` file.
You want to uncomment all the postgres lines in the application.conf file.

.. only:: html

    Before::

        //metastore.connection-postgresql.host = "invalid-postgresql-host"
        //metastore.connection-postgresql.port = 5432
        //metastore.connection-postgresql.database = "ia-metastore"
        //metastore.connection-postgresql.username = "iauser"
        //metastore.connection-postgresql.password = "myPassword"
        //metastore.connection-postgresql.url = "jdbc:postgresql://"${intel.analytics.metastore.connection-postgresql.host}":"${intel.analytics.metastore.connection-postgresql.port}"/"${intel.analytics.metastore.connection-postgresql.database}
        //metastore.connection = ${intel.analytics.metastore.connection-postgresql}

    After::

        metastore.connection-postgresql.host = "localhost"
        metastore.connection-postgresql.port = 5432
        metastore.connection-postgresql.database = "YOURDATABASE"
        metastore.connection-postgresql.username = "YOURUSER"
        metastore.connection-postgresql.password = "YOUR_PASSWORD"
        metastore.connection-postgresql.url = "jdbc:postgresql://"${intel.analytics.metastore.connection-postgresql.host}":"${intel.analytics.metastore.connection-postgresql.port}"/"${intel.analytics.metastore.connection-postgresql.database}
        metastore.connection = ${intel.analytics.metastore.connection-postgresql}

.. only:: latex

    Before::

        //metastore.connection-postgresql.host = "invalid-postgresql-host"
        //metastore.connection-postgresql.port = 5432
        //metastore.connection-postgresql.database = "ia-metastore"
        //metastore.connection-postgresql.username = "iauser"
        //metastore.connection-postgresql.password = "myPassword"
        //metastore.connection-postgresql.url = "jdbc:postgresql://"
            ${intel.analytics.metastore.connection-postgresql.host}":"
            ${intel.analytics.metastore.connection-postgresql.port}"/"
            ${intel.analytics.metastore.connection-postgresql.database}
        //metastore.connection = ${intel.analytics.metastore.connection-postgresql}

    After::

        metastore.connection-postgresql.host = "localhost"
        metastore.connection-postgresql.port = 5432
        metastore.connection-postgresql.database = "YOURDATABASE"
        metastore.connection-postgresql.username = "YOURUSER"
        metastore.connection-postgresql.password = "YOUR_PASSWORD"
        metastore.connection-postgresql.url = "jdbc:postgresql://"
            ${intel.analytics.metastore.connection-postgresql.host}":"
            ${intel.analytics.metastore.connection-postgresql.port}"/"
            ${intel.analytics.metastore.connection-postgresql.database}
        metastore.connection = ${intel.analytics.metastore.connection-postgresql}
        #comment any h2 configuration lines with a # or //::
         //metastore.connection = ${intel.analytics.metastore.connection-h2}

When you are done updating the application.conf file with the postgres
information restart the Intel Analytics service and insert the meta user into
the database::

    $ sudo service intelanalytics restart

After restarting the service, the |IA| will create all the database tables
after which we will insert a meta user to enable Python client requests.

Login to the postgres linux user::

    $ sudo su postgres

Open the postgres command line::

    $ psql

Switch databases::

    postgres=# \c YOURDATABASE
    psql (8.4.18)
    You are now connected to database "YOURDATABASE".

Then insert into the users table::

    postgres=# insert into users (username, api_key, created_on, modified_on) values( 'metastore', 'test_api_key_1', now(), now() );
    INSERT 0 1

After you get a confirmation of the insert you can send commands from the python client. you can view the insertion by doing a select on the users table

    postgres=# select * from users;

You should only get a single row per api_key::

     user_id | username  |    api_key     |         created_on         |        modified_on
    ---------+-----------+----------------+----------------------------+----------------------------
           1 | metastore | test_api_key_1 | 2014-11-20 12:37:16.535852 | 2014-11-20 12:37:16.535852
       (1 row)

If you get more than one row for a single api key remove one of them or create a new database. 
If you have duplicate api keys the server will not be able to validate request from the rest client. 
You should only have a single row per api key.

     user_id | username  |    api_key     |         created_on         |        modified_on
    ---------+-----------+----------------+----------------------------+----------------------------
           1 | metastore | test_api_key_1 | 2014-11-20 12:37:16.535852 | 2014-11-20 12:37:16.535852
           2 | metastore | test_api_key_1 | 2014-11-20 12:38:01.535852 | 2014-11-20 12:38:01.535852

    postgres=# insert into users (username, api_key, created_on, modified_on) values( 'metastore', 'test_api_key_1', now(), now() );
    INSERT 0 1


Starting |IA| REST Server
=========================

Starting the REST server is very easy.
It can be started like any other Linux service. ::

    sudo service intelanalytics start

After starting the rest server, you can browse to the host on port 9099
(<master node ip address>.9099) to see if the server started successfully.

Troubleshooting |IA| REST Server
================================

The log files get written to /var/log/intelanalytics/rest-server/output.log or
/var/log/intelanalytics/rest-server/application.log.
If you are having issues starting or running jobs, tail either log to see what
error is getting reported while running the task::

    sudo tail -f /var/log/intelanalytics/rest-server/output.log

or::

    sudo tail -f /var/log/intelanalytics/rest-server/application.log


More details about the logs can be found here: :doc:`ad_log`.

Upgrading Python to 2.7
=======================

Remove the old Python 2.7 client on all your nodes::

    sudo yum remove intelanalytics-python-rest-client-python27
 
Make sure your yum dependency repo is pointed to
https://intel-analytics-dependencies.s3-us-west-2.amazonaws.com/yum
Update/install intelanalytics on slave nodes::

    sudo yum install intelanalytics-python-rest-client intelanalytics-spark-deps
 
Update/install intel analytics rest server on master::

    sudo yum install intelanalytics-rest-server

.. toctree::
    :hidden:
    
    ad_inst_ia2
    ad_log

