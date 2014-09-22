==========================
IPython Setup Instructions
==========================

.. contents:: Table Of Contents
    :local:

------------
Introduction
------------

If you have a desire to use Intel Analytics server through IPython you are at the right place.
We will guide you through IPython setup needed to communicate with an Intel Analytics service on a remote cluster.
After reading and working through this guide you should be able to execute the Intel Analytics
python rest client from a remote host through IPyhon shell or notebook server. 

------------
Requirements
------------

Operating System Requirements 
=============================

These instructions will only cover the installing on `Red Hat Enterprise Linux <http://redhat.com/>`_ or
`CentOS <http://centos.org/>`_ version 6.

User Permission Requirements 
============================

Since you will be installing packages via the `yum <http://en.wikipedia.org/wiki/Yellowdog_Updater,_Modified>`_ command
you will need `sudo <http://en.wikipedia.org/wiki/Sudo>`_ command access to execute **'yum'** commands successfully.
To verify access run this quick command::

    $ sudo yum repolist

    # failed sudo yum verification:
    [sudo] password for jdoe:
    jdoe is not in the sudoers file. This incident will be reported.

If you don't have permissions to run the 'sudo' command you will see access denied errors,
and will need to contact your system administrator.
If you do have access to 'sudo' and 'yum' you will see a list of repositories.
::

    $ sudo yum repolist

    # successful sudo yum verification
    Loaded plugins: amazon-id, rhui-lb, s3
    repo id                           ...  repo name
    epel                              ...  Extra Packages for Enterprise Linux 6 - x86_64 ...
    rhui-REGION-client-config-server-6...  Red Hat Update Infrastructure 2.0 Client Config...
    rhui-REGION-rhel-server-releases  ...  Red Hat Enterprise Linux Server 6 (RPMs)       ...
    rhui-REGION-rhel-server-releases-o...  Red Hat Enterprise Linux Server 6 Optional (RPM...
    rhui-REGION-rhel-server-rh-common ...  Red Hat Enterprise Linux Server 6 RH Common (RP...
    repolist: 31,335

Cluster Requirements
====================

You need a working Intel Analytics cluster installation configured to run with the python2.7 executable.
If you are unsure Ask your system administrator.
If you installed Intel Analytics on the cluster refer to the installation documentation to make the necessary changes.

You need the hostname of the node that is running the Intel Analytics service.
If the service is not using the default 9099 port, you also need the port number on which the service is running.

-------------------------
Adding Extra Repositories
-------------------------

The first step in the installation is adding `EPEL <https://fedoraproject.org/wiki/EPEL>`_,
and two Intel Analytics repositories to make the installation as smooth and painless as it can be on Linux.

Add EPEL Repository
===================

Before trying to install the EPEL repository, run the following command to see if it's already available
on the machine you will be working on::

    $ sudo yum repolist

    # sample output
    repo id                           ...  repo name
    cloudera-cdh5                     ...  Cloudera CDH, Version 5                        ...
    cloudera-manager                  ...  Cloudera Manager, Version 5.1.0                ...
    epel                              ...  Extra Packages for Enterprise Linux 6 - x86_64 ...
    rhui-REGION-client-config-server-6...  Red Hat Update Infrastructure 2.0 Client Config...
    rhui-REGION-rhel-server-releases  ...  Red Hat Enterprise Linux Server 6 (RPMs)       ...
    rhui-REGION-rhel-server-releases-o...  Red Hat Enterprise Linux Server 6 Optional (RPM...
    rhui-REGION-rhel-server-rh-common ...  Red Hat Enterprise Linux Server 6 RH Common (RP...

You want to look for the *'epel'* repo id.
If you don't see the *'epel'* repo id, execute the following commands to install it::

    $ wget http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
    $ sudo rpm -ivh epel-release-6-8.noarch.rpm

To verify the EPEL repository installation, run::

    $ sudo yum repolist

    # sample output
    repo id                           ...  repo name
    cloudera-cdh5                     ...  Cloudera CDH, Version 5                        ...
    cloudera-manager                  ...  Cloudera Manager, Version 5.1.0                ...
    epel                              ...  Extra Packages for Enterprise Linux 6 - x86_64 ...
    rhui-REGION-client-config-server-6...  Red Hat Update Infrastructure 2.0 Client Config...
    rhui-REGION-rhel-server-releases  ...  Red Hat Enterprise Linux Server 6 (RPMs)       ...
    rhui-REGION-rhel-server-releases-o...  Red Hat Enterprise Linux Server 6 Optional (RPM...
    rhui-REGION-rhel-server-rh-common ...  Red Hat Enterprise Linux Server 6 RH Common (RP...

Make sure the *'epel'* repo id is present.

Add Intel Analytics Dependency Repository
=========================================

We pre-package and host some open source libraries to aid with installation.
In some cases we pre-packaged newer versions from what is available in RHEL or EPEL repositories.

To add the Intel Analytics dependency repository run the following command::

    $ wget https://intel-analytics-dependencies.s3-us-west-2.amazonaws.com/ia-deps.repo

    $ sudo cp ia-deps.repo /etc/yum.repos.d/

If you have issues running the above command try::

    $ sudo touch /etc/yum.repos.d/ia-deps.repo
    $ echo "[intel-analytics-deps]
    name=intel-analytics-deps
    baseurl=https://intel-analytics-dependencies.s3-us-west-2.amazonaws.com/yum
    gpgcheck=0
    priority=1
    enabled=1"  | sudo tee -a /etc/yum.repos.d/ia-deps.repo

To test the repository configuration run the following command::

    $ sudo yum info yum-s3
    # should print something close to this
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

Add Intel Analytics Private Repository
======================================

Next we will create /etc/yum.repos.d/ia.repo.
Don't forget to replace "YOUR_KEY", and "YOUR_SECRET" with your given AWS access, and secret keys.

Run the following command to create /etc/yum.repos.d/ia.repo file::

    $ echo "[intel-analytics]
    name=intel analytics
    baseurl=https://intel-analytics-repo.s3-us-west-2.amazonaws.com
        /release/latest/yum/dists/rhel/6
    gpgcheck=0
    priority=1
    s3_enabled=1
    #yum-get iam only has get
    key_id=YOUR_KEY
    secret_key=YOUR_SECRET" | sudo tee -a /etc/yum.repos.d/ia.repo

The ``baseurl`` line above has been broken across two lines for displaying in various media.
The lines should be combined into a single line with no gaps (spaces).

.. note::

    Don't forget to replace YOUR_KEY, and YOUR_SECRET with the keys that were given to you.

Verify the IA repository configuration by running::

    $ sudo yum info intelanalytics-rest-server

    # sample output
    Available Packages
    Name     : intelanalytics-rest-server
    Arch     : x86_64
    Version  : 0.8
    Release  : 1474
    Size     : 419 M
    Repo     : intel-analytics
    Summary  : intelanalytics-rest-server-0.8 Build number: 1474. TimeStamp 20140722...
    URL      : graphtrial.intel.com
    License  : Confidential

If you get package details for intelanalytics-rest-server package then the repository installed correctly and
you can continue installation.

Troubleshooting Private Repository
==================================

The most common error when using the private repository is incorrect access and secret keys or
the server time is out of sync with the world.
It never hurts to double check your access and secret keys in the ia.repo file.

To keep your system time in sync run::

    sudo service ntpd start

------------
Installation
------------

Installing Intel Analytics Python Rest Client
=============================================

Now that we have all the 'yum' repositories configured We can go ahead and install the Intel Analytics
python rest client for python 2.7.
If you don't already have python 2.7 installed it will be installed automatically since python 2.7 is
a dependency on Intel Analytics python rest client.
Run the following command to install the Intel Analytics rest client and all it's dependent packages::

    sudo yum install intelanalytics-python-rest-client-python27

Installing IPython
==================

To install Ipython run::

    $ sudo yum install python27-ipython

--------------------------------------------
Configure Intel Analytics Python Rest Client
--------------------------------------------

Before you fire up IPython you need to configure the Intel Analytics rest client.
We need to let the rest client know where to find the Intel Analytics rest server by updating the
host address in /usr/lib/intelanalytics/rest-client/python/rest/config.py::

    $ sudo vim /usr/lib/intelanalytics/rest-client/python/rest/config.py

Your config.py file will look similar to this::

    """
    config file for rest client
    """
    # default connection config
    class server:
        host = "localhost"
        port = 9099
        scheme = "http"
        version = "v1"
        headers = {'Content-type': 'application/json',
                'Accept': 'application/json,text/plain',
                'Authorization': "test_api_key_1"}

    class polling:
        start_interval_secs = 1
        max_interval_secs = 20
        backoff_factor = 1.02

    build_id = None

We want to update the address for host to the `Fully Qualified Domain Name
<http://en.wikipedia.org/wiki/Fully_qualified_domain_name>`_ or
IP address of the node hosting the Intel Analytics rest server.

---------------
Running IPython
---------------

You should now able to open an ipython shell or notebook server.
Test the Intel Analytics IPython install by importing the rest client libraries inside of a notebook or
Ipython shell and trying to ping the rest server.
::

    # testing IPython/Intel Analytics
    
    $ ipython
    Python 2.7.5 (default, Sep  4 2014, 17:06:50)
    Type "copyright", "credits" or "license" for more information.
    IPython 2.2.0 -- An enhanced Interactive Python.
    ?         -> Introduction and overview of IPython's features.
    %quickref -> Quick reference.
    help      -> Python's own help system.
    object?   -> Details about 'object', use 'object??' for extra details.

    In [1]: from intelanalytics import *

    In [2]: server.ping()
    Successful ping to Intel Analytics at http://localhost:9099/info

    In [3]:

IPython Notebook
================

All the dependencies to run the IPython notebook server are also installed which lets you run
the IPython shell from a web browser.
You can start the notebook server with the following command::
    
    $ ipython notebook

