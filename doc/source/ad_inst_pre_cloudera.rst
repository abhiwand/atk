==============================
Physical Machine Configuration
==============================

.. contents:: Table of Contents
    :local:

The following instructional will walk you through configuring a brand new physical machine from scratch.
The first section called "Pre-Configuration" and will be a little vague because the configurations here can change from machine to machine.
The second section called "Base Configuration" should be pretty identical for all machines.

-----------------
Pre-Configuration
-----------------

1. Configure basic network connectivity
#. Turn off IPV6 (many different ways to do this)
#. If applicable, mount any large volume to /mnt* vi /etc/fstab

------------------
Base Configuration
------------------

1. Configure client-side DNS:
=============================
Insure all systems in cluster are DNS or /etc/hosts resolvable.

2. Disable Firewall
===================
::

    service iptables stop
    chkconfig iptables off

3. Disable SELINUX
==================
::

    vi /etc/selinux/config

Change::

    SELINUX=enforcing

To::        

    SELINUX=disabled

Or::

    SELINUX=permissive

Note:
    Systems will need full reboot before changes take effect.

4. Enable system default repos
==============================
Make sure system default yum repos are functional.

5. Configure system proxy settings (if necessary)
=================================================
If working behind a proxy, make sure system proxy settings are configured.

6. Insure system packages are sync'ed with default repositories
===============================================================
::

    yum clean all
    yum distro-sync

.. ifconfig:: internal_docs

    12. Install Basic Development Tools
    ===================================

    RedHat/CentOS/Oracle Linux:
    ---------------------------
    ::

        yum --disableexcludes=all install kernel-headers kernel-devel
        yum groupinstall "Development tools"

    13. Install/Update Additional Software
    ======================================

    RedHat/CentOS:
    ---------------------------
    ::

        yum install openssl openssh man curl nc nano screen vim-enhanced grep gawk awk service chkconfig ntp rpm python sudo rsync

7. Determine primary CDH user
=============================

Cloudera supports use of root or sudo user as administration user
If using sudo insure user has full nopassword sudo privileges.

8. Insure proper SSH connections
================================
Insure that all systems in cluster can SSH between one-another using the administrative user previously determined.

9. Set desired hostname of system
=================================
Set all hostname for systems in cluster.

Note:
    We recommend limiting host names to lower case alpha-numeric characters.

10. Set ulimits in /etc/security/limits.conf
============================================
Insure the following definitions exist in /etc/security/limits.conf

::

    vi /etc/security/limits.conf

    *                soft    nofile          32768
    *                hard    nofile          32768
    hadoop           -       nofile          32768
    hadoop           -       nproc           unlimited
    hdfs             -       nofile          32768
    hbase            -       nofile          32768
    spark            soft    nofile          65535
    spark            hard    nofile          65535
    spark            -       nproc           32768


11. Configure NTP
=================
Insure NTP is insttalled and properly configured on all cluster systems.
Also insure NTP service starts on system boot::

    service ntpd start
    chkconfig ntpd on

Make sure all systems in cluster are in time-sync with one-another.

12. Reboot!
===========

reboot all cluser systems to properly set all changes made.
