=======================
Set Up A Cluster Server
=======================

A cluster server is usually a dedicated group of computers, but for testing and trial uses, it could be the same computer as the client.

------------------
Set Up Server User
------------------
A user needs to be set up on the server with superuser rights without a password, for example ``hadoop``::

    sudo visudo

Add the following line to the end of file::

    hadoop ALL=(ALL)      NOPASSWD: ALL

-----------
Set up Host
-----------
Use a fully qualified hostname wherever it can be used, for example ``host.computercenter.com``.
To assign a hostname to the workstation:

+---------------------------------------------------------------+---------------------------------------------------------------+
| RedHat/CentOS                                                 | Ubuntu                                                        |
+===============================================================+===============================================================+
| edit the file: 'etc/sysconfig/network'                        | edit the file: '/etc/hostname'                                |
+---------------------------------------------------------------+---------------------------------------------------------------+

Add or change the hostname to: ``host.computercenter.com``

Save the change

Run the command::

    sudo hostname ``host.computercenter.com``

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

This sets up a data server.

.. _Cloudera Manager: http://www.cloudera.com/content/support/en/downloads/cloudera_manager/cm-5-0-2.html
.. _Cloudera Documentation: http://www.cloudera.com/content/support/en/documentation/cdh5-documentation/cdh5-documentation-v5-latest.html
