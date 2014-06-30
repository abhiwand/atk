====================
Set Up A Workstation
====================

-----------
Set Up User
-----------
A user needs to be set up on the workstation with superuser rights without a password, for example ``hadoop``::

    sudo visudo

Add the following line to the end of file::

    hadoop ALL=(ALL)      NOPASSWD: ALL

-----------
Set up Host
-----------
The next step is to set up the hosts file.
    Edit the file /etc/hosts

    Add the actual ip address and then the hostname, for example: ``127.0.0.1`` ``host.computercenter.com``

    The actual hostname should be just after the ip address in a single line.

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

