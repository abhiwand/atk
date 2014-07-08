===============
Set Up A Server
===============

A cluster server is usually a dedicated group of computers, but for testing and trial uses, it could be the same computer as the client.

-----------
Server User
-----------
A user needs to be set up on the server with superuser rights without a password, for example ``hadoop``::

    sudo visudo

Add the following line to the end of file::

    hadoop ALL=(ALL)      NOPASSWD: ALL

----
Host
----
Use a fully qualified hostname wherever it can be used, for example ``host.computercenter.com``.
To assign a hostname to the workstation:

+---------------------------------------------------------------+---------------------------------------------------------------+
| RedHat/CentOS                                                 | Ubuntu                                                        |
+===============================================================+===============================================================+
| edit the file: 'etc/sysconfig/network'                        | edit the file: '/etc/hostname'                                |
+---------------------------------------------------------------+---------------------------------------------------------------+
| Add or change the hostname to: <your hostname>                | Add or change the hostname to: <your hostname>                |
+---------------------------------------------------------------+---------------------------------------------------------------+
| Save the change                                               | Save the change                                               |
+---------------------------------------------------------------+---------------------------------------------------------------+
| Run the command::                                             | Run the command::                                             |
|                                                               |                                                               |
|     sudo hostname <your hostname>                             |     sudo hostname <your hostname>                             |
+---------------------------------------------------------------+---------------------------------------------------------------+

The next step is to set up the hosts file.
    Edit the file: '/etc/hosts'

    Add the actual IP address and then the hostname, for example: ``10.10.68.32`` ``host.computercenter.com``

    The hostname should be just after the IP address in a single line.

--------
IPTables
--------
Now, turn off iptables.

+---------------------------------------------------------------+---------------------------------------------------------------+
| RedHat/CentOS                                                 | Ubuntu                                                        |
+===============================================================+===============================================================+
| ::                                                            | ::                                                            |
|                                                               |                                                               |
|     sudo service iptables stop                                |     sudo service ufw stop                                     |
|     sudo chkconfig iptables off                               |     sudo ufw disable                                          |
+---------------------------------------------------------------+---------------------------------------------------------------+

-------
SELinux
-------
And turn off selinux.

    Edit the file: '/etc/sysconfig/selinux'

    Change the "SELINUX" line to: "SELINUX=disabled"

------
Defrag
------
If you are running on RedHat, keep it from defragmenting the hard drive.

    Edit the file: /etc/rc.local

    At the bottom of the file enter: echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag

----------------
Cloudera Manager
----------------
Install the Cloudera Manager. See `Cloudera Manager`_ and `Cloudera Documentation`_.

-----------------------------
Platform Specific Information
-----------------------------

:doc:`ad_yum_se`

:doc:`ad_apt_se`

-----------
REST Server
-----------

Look in the file '/etc/default/intelanalytics-rest-server'. There is a line which looks like "export SPART_HOME=...".
If this line has a comment symbol "#" at the beginning, it needs to be removed to make it active.
The part after the "=" symbol should be the path to where SPARK is installed.
If it is different on your system, change the path to match your system.

Zookeeper is required to be running on the Server.
Edit the file '/etc/intelanalytics/rest-server/reference.conf'.
Under the section "titan.load.storage", change the line hostname = "localhost" to hostname = "node01, node02, node03" where node01, node02
and node03 are the nodes that have the Zookeper role assigned to them.

-------------------
Running REST Server
-------------------

Local Mode
==========

Start the intelanalytics-rest-server::

    sudo service intelanalytics-rest-server start

Cluster Mode
============
Login to each node and install "intelanalytics-python-rest-client" as follows::

    sudo yum install intelanalytics-python-rest-client

Open the file "/etc/intelanalytics/rest-server/application.conf" and make the following changes:

    Under the section titled "intel.analytics.spark"::

        master = "spark://<HOST_NAME>:7077"
        home = "/opt/cloudera/parcels/CDH/lib/spark"

        Comment out home = "" and master = "local[4]" lines.

    Under the section titled "spary.can.server"::

        Set request-timeout = 29s (otherwise you won't be able to build large frames)

    Under the section titled "intelanalytics.fs" change the following::

        root = "hdfs://<MASTER_NODE_HOST_NAME>/user/hadoop"

    Comment out the line::
    
        root = ${user.home}

To give write permission to hadoop for HDFS access, run the command::

    hadoop fs -chmod -R 777 /user/hadoop/

Start the intelanalytics-rest-server::

    sudo service intelanalytics-rest-server start


.. _Cloudera Manager: http://www.cloudera.com/content/support/en/downloads/cloudera_manager/cm-5-0-2.html
.. _Cloudera Documentation: http://www.cloudera.com/content/support/en/documentation/cdh5-documentation/cdh5-documentation-v5-latest.html
