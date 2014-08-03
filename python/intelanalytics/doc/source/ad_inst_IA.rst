===============
Set Up A Server
===============

------------
Introduction
------------

In this guide we will walk through the Intel Analytics installation and the minimal configuration needed to get the service running.
This guide is not going to walk you through the Cloudera cluster installation since that subject is covered by Cloudera in greater detail.
For brevity Intel Analytics will be referred to as IA going forward.

See `Cloudera Installation Documentation <http://www.cloudera.com/content/cloudera-content/cloudera-docs/CM5/latest/Cloudera-Manager-Installation-Guide/cm5ig_install_cm_cdh.html>`_

------------
Requirements
------------

1. RHEL/Centos 6.4 OS
#. Sudo access is required to install the various IA packages since they are installed through Yum and for the editing of root owned configuration files
#. Cloudera cluster with CDH-5.0.2-p0.13 or greater up to CDH-5.0.3-p0.35 with the following services installed and running.

    a. HDFS
    #. SPARK
    #. Hbase
    #. Yarn(MR2)
    #. Zookeeper

#. Python 2.6
#. EPEL yum repository -- All the nodes on the cluster must have the EPEL yum repository.
   Adding the EPEL repository is straight forward and can be accomplished with a few simple steps.

Before trying to install the EPEL repo run the following command to see if it's already available on the machine you are working on::

    sudo yum repolist

    cloudera-cdh5                              Cloudera CDH, Version 5                                              141
    cloudera-manager                           Cloudera Manager, Version 5.0.2                                        7
    epel                                       Extra Packages for Enterprise Linux 6 - x86_64                    11,022
    rhui-REGION-client-config-server-6         Red Hat Update Infrastructure 2.0 Client Configuration Server 6        2
    rhui-REGION-rhel-server-releases           Red Hat Enterprise Linux Server 6 (RPMs)                          12,690
    rhui-REGION-rhel-server-releases-optional  Red Hat Enterprise Linux Server 6 Optional (RPMs)                  7,168

If the epel repo is missing, run these commands to install the necessary files::

    wget http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
    rpm -ivh epel-release-6-8.noarch.rpm

To verify the installation run::

    sudo yum repolist

    epel                                             Extra Packages for Enterprise Linux 6 - x86_64                       11,018
    rhui-REGION-client-config-server-6               Red Hat Update Infrastructure 2.0 Client Configuration Server 6           2
    rhui-REGION-rhel-server-releases                 Red Hat Enterprise Linux Server 6 (RPMs)                             12,663
    rhui-REGION-rhel-server-releases-optional    

    Red Hat Update Infrastructure 2.0 Client Configuration Server 6                       rhui-REGION-rhel-server-releases
    Red Hat Enterprise Linux Server 6 (RPMs)                                              12,663rhui-REGION-rhel-server-releases-optional
    Red Hat Enterprise Linux Server 6 Optional (RPMs)                                      7,16


------------------------
Intel Analytics packages
------------------------

The dependency list is merely informational.
When yum installs a package, it will pull dependencies automatically.
All the Cloudera dependencies are implied for all packages.

IA Rest Server
==============

Package Name: intelanalytics-rest-server

Dependencies

* intelanalytics-python-client
* intelanalytics-graphbuilder
* Java Runtime Environment or Java Development Environment 1.7

IA Python Client
================

Needs to be installed on every spark worker node as well as the gateway node or other machine that is going to be the designated client.
The version must be the same on the IA python client submitting requests, the rest server and the rest client installed on the individual nodes.


Package Name: intelanalytics-python-client

Dependencies

* python 2.6
* python-ordereddict
* python-pip
* numpy >= 1.8.1
* python-pandas >= 0.13.1
* python-bottle >= 0.12
* python-requests >= 2.2.1

IA Graph Builder
================

Needs to be installed with the IA rest server

Package Name: intelanalytics-graphbuilder

Dependencies

* intelanalytics-spark-deps
* jq *
* perl-URI *

\* These dependenicies are only used in a helper script that updates the spark class path in Cloudera manager.
They are not used for any data processing.

IA Spark Dependencies
=====================

Need to be installed on every spark worker node.

Package Name: intelanalytics-spark-deps

Dependencies

* none


------------
Installation
------------

Both 'Intel-analytics-deps' and 'intel-analytics' repositories need to be installed on every node that has a spark worker.


Add Dependency Repository
=========================

We pre-package and host some open source libraries to aid with installations.
In some cases we pre-packaged newer versions from what is available in RHEL or EPEL repositories.

To add the dependency repository run the following command::

    wget https://intel-analytics-dependencies.s3-us-west-2.amazonaws.com/ia-deps.repo

    sudo cp ia-deps.repo /etc/yum.repos.d/

If you have issues running the above command, try entering the following, being careful about the placement of the \" characters::

    sudo touch /etc/yum.repos.d/ia-deps.repo
    echo "[intel-analytics-deps]
    name=intel-analytics-deps
    baseurl=https://intel-analytics-dependencies.s3-us-west-2.amazonaws.com/yum
    gpgcheck=0
    priority=1 enabled=1"  | sudo tee -a /etc/yum.repos.d/ia-deps.repo

To test the installation of the dependencies repository run the following command::

    sudo yum info yum-s3

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


If you get a similar output install yum-s3 package::

    sudo yum -y install yum-s3

Add Private repository
======================

Copy and paste these contents to '/etc/yum.repos.d/ia.repo'.
If the file doesn't exist create it.
The name of the file doesn't matter as long as it has the .repo file extension.
::

    [intel-analytics]
    name=intel analytics
    **baseurl=https://intel-analytics-repo.s3-us-west-2.amazonaws.com/release/0.8.0/yum/dists/rhel/6 **
    gpgcheck=0
    priority=1
    s3_enabled=1
    #yum-get iam only has get
    key_id=YOUR_KEY
    secret_key=YOUR_SECRET

Alternatively you can run::

    echo "[intel-analytics]
    name=intel analytics
    **baseurl=https://intel-analytics-repo.s3-us-west-2.amazonaws.com/release/0.8.0/yum/dists/rhel/6 **
    gpgcheck=0
    priority=1
    s3_enabled=1
    #yum-get iam only has get
    key_id=YOUR_KEY
    secret_key=YOUR_SECRET" | sudo tee -a /etc/yum.repos.d/ia.repo

Note:
    Don't forget to replace YOUR_KEY, and YOUR_SECRET with the keys that were given to you.

Verify the installation of the IA repository by running::

    sudo yum info intelanalytics-rest-server

    Available Packages
    Name        : intelanalytics-rest-server
    Arch        : x86_64
    Version     : 0.8
    Release     : 1474
    Size        : 419 M
    Repo        : intel-analytics
    Summary     : intelanalytics-rest-server-0.8 Build number: 1474. TimeStamp 20140722211530Z
    URL         : graphtrial.intel.com
    License     : Confidential

If you get package details for intelanalytics-rest-server package, then the repository installed correctly and you can continue installation.

--------------
IA rest server
--------------

This next step is going to install IA rest server and all it's dependencies.
Only one instance of the rest server needs to be installed.
Although it doesn't matter where it's installed, it's usually installed on the same node where spark master is running.
::

    sudo yum -y install intelanalytics-rest-server

Configuration
=============

Before starting the server you must edit two config files /etc/default/intelanalytics-rest-server,
/etc/intelanalytics/rest-server/application.conf.tpl.

/etc/default/intelanalytics-rest-server:
----------------------------------------

In /etc/default/intelanalytics-rest-server we need to set ``spark_home`` to the correct location according to your Cloudera installation.
If you open the file it will look something like this::

    #intelanalytics-rest-server env file
    #Set all your environment variables needed for the rest server here
    
    # depending on the CDH install method used, set the appropriate SPARK_HMOE below

    #export SPARK_HOME="/usr/lib/spark"
    **export SPARK_HOME="/opt/cloudera/parcels/CDH/lib/spark"**

    export IA_JVM_OPT="-XX:MaxPermSize=256m"
    export EXTRA_CONF=`hbase classpath`
    export IAUSER="iauser"
    export HOSTNAME=`hostname`

If your Cloudera cluster is parcel-based, the default SPARK_HOME should work.
If your Cloudera cluster is packaged-based, like RPM or DEB, use "/usr/lib/spark".

/etc/intelanalytics/rest-server/application.conf.tpl:
-----------------------------------------------------

The rest-server package only provides a configuration template called application.conf.tpl.
**We need to copy and rename this file to application.conf and update host names and memory configurations.**
First let's rename the file::

    sudo cp /etc/intelanalytics/rest-server/application.conf.tpl /etc/intelanalytics/rest-server/application.conf

Open the file with your editor of choice (we use vim for example)::

    sudo vim /etc/intelanalytics/rest-server/application.conf

**All the changes that need to be made are at the top of the file.
This is the section you want to look at::

    # BEGIN REQUIRED SETTINGS

    intel.analytics {

        # The host name for the Postgresql database in which the metadata will be stored
        metastore.connection-postgresql.host = "invalid-postgresql-host"

        engine {

            # The hdfs URL where the intelanalytics folder will be created
            # and which will be used as the starting point for any relative URLs
            fs.root = "hdfs://invalid-fsroot-host/user/iauser"

            # The (comma separated, no spaces) Zookeeper hosts that
            # Titan needs to be able to connect to HBase
            titan.load.storage.hostname = "invalid-titan-host"
            titan.query.storage.hostname = ${intel.analytics.engine.titan.load.storage.hostname}

            spark {
                # The URL for connecting to the Spark master server
                master = "spark://invalid-spark-master:7077"

                conf.properties {
                    # Memory should be same or lower than what is listed as available in Cloudera Manager.
                    # Values should generally be in gigabytes, e.g. "8g"
                    spark.executor.memory = "invalid executor memory"
                }
            }
        }
    }

    # END REQUIRED SETTINGS

1. Configure meta store

Comment the following line by pre-pending it with '//'::

    metastore.connection-postgresql.host = "invalid-postgresql-host"
    
and replace it with this::

    //metastore.connection-postgresql.host = "invalid-postgresql-host"
    metastore.connection = ${intel.analytics.metastore.connection-h2}

2. Configure file system root

In the following line the text "invalid-fsroot-host" should be replaced with the fully qualified domain of your HDFS installation::

    fs.root = "hdfs://invalid-fsroot-host/user/iauser"
    
3. Configure zookeeper host

In the following line replace "invalid-titan-host" with the comma delimited list of fully qualified domain names of all nodes running the zookeeper service::

    titan.load.storage.hostname = "invalid-titan-host"
    
4. Configure spark host

Update "invalid-spark-master" with the fully qualified domain name of the spark master node::

    master = "spark://invalid-spark-master:7077"
    
5. Configure spark executor memory

The spark executor memory needs to be set equal to or less than what is configured in Cloudera manager.
The Cloudera Spark installation will, by default, set the spark executor memory to 8g, so 8g is usually a safe setting.
If have any doubts you can always verify the executor memory in Cloudera manager.
::

    spark.executor.memory = "invalid executor memory"

Click on the spark service then configuration in Cloudera manager to get executor memory.**

.. image:: ad_inst_IA_1.png
    :width: 80%
    :align: center


IA spark deps:
--------------

After setting up the IA repositories, run the following command on every host with a spark worker::

    sudo yum -y install intelanalytics-spark-deps

IA python rest client:
----------------------

After setting up the IA repositories, run the following command on every host with a spark worker::

    sudo yum -y install intelanalytics-python-rest-client

After installing IA spark deps and IA python rest client, you can start the rest server and start submitting requests.


Starting IA Rest Server:
------------------------

Starting the Rest server is very easy.
It can be started like any other linux service::

    sudo service intelanalytics-rest-server start

After starting the rest server, you can browse to the host on port 9099 to see if the server started successfully.

Troubleshooting:
----------------

The log files get written to /var/log/intelanalytics/rest-server/output.log or /var/log/intelanalytics/rest-server/application.log.
If you are having issues starting or running jobs, tail either log to see what error is getting reported while running the task::

    sudo tail -f /var/log/intelanalytics/rest-server/output.log

or::

    sudo tail -f /var/log/intelanalytics/rest-server/application.log

