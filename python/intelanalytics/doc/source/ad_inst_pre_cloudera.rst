==============================
Physical Machine Configuration
==============================

The following instructional will walk you through configuring a brand new physical machine from scratch in the JF2-H8 lab.
Some of these steps are specific to physical machines in the H8 lab and should not be used word for word for other types of system configurations.
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
::

    vi /etc/resolv.conf

    search clustername.bda bda
    nameserver 127.0.0.1

    search hf.intel.com
    nameserver 10.248.2.1
    nameserver 10.22.224.196
    nameserver 10.3.86.116

2. Make sure only localhost information exists in /etc/hosts:
=============================================================
::

    vi /etc/hosts

    127.0.0.1 localhost.localdomain   localhost
    ::1       localhost6.localdomain6 localhost6

3. Disable Firewall
===================

RedHat/CentOS/Oracle Linux:
---------------------------
::

    service iptables stop
    chkconfig iptables off

Ubuntu:
-------
::

    apt-get update
    apt-get install chkconfig
    ufw disable
    chkconfig ufw off

Debian:
-------
::

    apt-get update
    apt-get install chkconfig
    /etc/init.d/iptables stop
    chkconfig iptables off

SuSE:
-----
::

    SuSEfirewall2 off
    service SuSEfirewall2_init off
    service SuSEfirewall2_setup off

4. Disable SELINUX
==================

RedHat/CentOS/Oracle Linux:
---------------------------
::

    vi /etc/selinux/config

Change::

    SELINUX=enforcing

To::        

    SELINUX=disabled

Ubuntu/Debian/SuSE:
-------------------
::

    echo "0" > /selinux/enforce

5. Disable all unnecessary repos
================================

RedHat/CentOS/Oracle Linux:
---------------------------
::

    mkdir /etc/yum.repos.d/block
    mv /etc/yum.repos.d/*.repo /etc/yum.repos.d/block

Ubuntu/Debian (comment out all unnecessary software sources):
-------------------------------------------------------------
::

    vi /etc/apt/sources.list

SuSE:
-----
::

    mkdir /etc/zypp/repos.d/block
    mv /etc/zypp/repos.d/*.repo /etc/zypp/repos.d/block

6. Enable OS repo (NO Intel H8 RedHat6.2/CentOS6.2/Ubuntu/Debian/OracleLinux/SuSE Repos yet!)
=============================================================================================

RedHat 6.4:
-----------
::

    vi /etc/yum.repos.d/redhat-h8.repo

    [rhel-h8-server]
    name=RHEL 6.4 - H8 Server (RPMs)
    baseurl=http://10.54.8.247/Intel/pub/ISO/redhat/redhat-rhel6/RHEL-6.4-GA/Server/x86_64/os/
    enabled=1
    gpgcheck=0
    proxy=_none_

    [rhel-h8-client]
    name=RHEL 6.4 - H8 Client (RPMs)
    baseurl=http://10.54.8.247/Intel/pub/ISO/redhat/redhat-rhel6/RHEL-6.4-GA/Client/x86_64/os
    enabled=1
    gpgcheck=0
    proxy=_none_

    [rhel-h8-opt]
    name=RHEL 6.4 - H8 Client/Optional (RPMs)
    baseurl=http://10.54.8.247/Intel/pub/ISO/redhat/redhat-rhel6/RHEL-6.4-GA/Client/optional/x86_64/os
    enabled=1
    gpgcheck=0
    proxy=_none_

    

OPTIONAL - Download EPEL repo::

    rpm -Uvh http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm

CentOS 6.4:
-----------
::

    vi /etc/yum.repos.d/centos-h8.repo

    [centos-h8-server]
    name=CentOS 6.4 - H8 Server (RPMs)
    baseurl=http://10.54.8.247/CentOS/6/os/x86_64/
    enabled=1
    gpgcheck=0
    proxy=_none_

SuSE:
-----
::

    vi /etc/zypp/repos.d/opensuse.repo

    [openSuSE_11.3_OSS]
    name=openSuSE 11.3 OSS
    baseurl=http://ftp5.gwdg.de/pub/opensuse/discontinued/distribution/11.3/repo/oss/suse/
    enabled=1
    gpgcheck=0

Or::

    [openSuSE_11.2_OSS]
    name=openSuSE 11.2 OSS
    baseurl=http://ftp5.gwdg.de/pub/opensuse/discontinued/distribution/11.2/repo/oss/suse/
    enabled=1
    gpgcheck=0

Or::

    [openSuSE_11.1_OSS]
    name=openSuSE 11.1 OSS
    baseurl=http://ftp5.gwdg.de/pub/opensuse/discontinued/distribution/11.1/repo/oss/suse/
    enabled=1
    gpgcheck=0

7. Add the following proxy environment variable declaration lines to the end of /etc/bashrc
===========================================================================================
::

    vi /etc/bashrc

    ## BDA-INF Proxy Environment Variables
    #######################################
    export http_proxy=http://proxy.jf.intel.com:911
    export https_proxy=http://proxy.jf.intel.com:911
    export ftp_proxy=http://proxy.jf.intel.com:911
    export no_proxy=localhost,127.0.0.0/8,intel.com,.intel.com,10.0.0.0/8,192.168.0.0/16,134.134.0.0/16,10.23.61.0/16,cluster,.cluster,10.54.8.247

8. Add proxy information to package manager config
===================================================================================
(only if accessing outside repo)

RedHat/CentOS/Oracle Linux:
---------------------------
::

    vi /etc/yum.conf

    proxy=http://proxy.jf.intel.com:911

Ubuntu/Debian:
--------------
::

    vi /etc/apt/apt.conf

    Acquire::http::Proxy "http://proxy.jf.intel.com:911";

SuSE (No further config required):
----------------------------------

9. Update package manager
=========================

RedHat/CentOS/Oracle Linux:
---------------------------
::

    yum clean all
    yum update yum

Ubuntu/Debian:
--------------
::

    apt-get update
    apt-get upgrade apt

SuSE:
-----
::

    zypper refresh
    zypper update zypper

10. If RedHat/CentOS/Oracle Linux, disable kernel/distro package upgrades
=========================================================================
::

    vi /etc/yum.conf

    exclude=kernel* dracut* *-release *-release-server *-release-notes

11. Update all other system software
====================================

RedHat/CentOS/Oracle Linux:
---------------------------
::

    yum update

Ubuntu/Debian:
--------------
::

    apt-get upgrade

SuSE:
-----
::

    zypper update

12. Install Basic Development Tools
===================================

RedHat/CentOS/Oracle Linux:
---------------------------
::

    yum --disableexcludes=all install kernel-headers kernel-devel
    yum groupinstall "Development tools"

Ubuntu/Debian:
--------------
::

    apt-get install build-essential

SuSE:
-----
::

    zypper install gcc g++ make automake autoconf

13. Install/Update Additional Software
======================================

RedHat/CentOS/Oracle Linux:
---------------------------
::

    yum install openssl openssh man curl nc nano screen vim-enhanced grep gawk awk service chkconfig ntp rpm python sudo rsync

Ubuntu/Debian:
--------------
::

    apt-get install openssl openssh-client openssh-server man curl netcat nano screen vim grep gawk chkconfig ntp python sudo rsync

SuSE:
-----
::

    zypper install openssl openssh man curl nc nano screen vim-enhanced grep gawk awk service chkconfig ntp rpm python sudo rsync

14. Seed /etc/skel with base SSH junk
=====================================
::

    mkdir /etc/skel/.ssh
    chmod 700 /etc/skel/.ssh
    vi /etc/skel/.ssh/authorized_keys

    Copy/Paste public key (should already be created on management system)

    chmod 600 /etc/skel/.ssh/authorized_keys

15. Create gaoadm and hadoop user and group
===========================================
::

    groupadd gaoadm
    useradd -m -g gaoadm gaoadm
    groupadd hadoop
    useradd -m -g hadoop hadoop

16. Add gaoadm and hadoop users to sudoers
==========================================
::

    visudo

    ## BDA-INF Sudo Users
    ######################
    gaoadm        ALL=(ALL)       NOPASSWD: ALL
    hadoop        ALL=(ALL)       NOPASSWD: ALL

17. Test SSH key login to hadoop user
=====================================

(may require logging out and back in as hadoop if SSH'd into system)

18. Set hostname of system
==========================

RedHat/CentOS/Oracle Linux:
---------------------------
::

    vi /etc/sysconfig/network

    HOSTNAME=localhost.localdomain

Ubuntu/Debian:
--------------
::

    vi /etc/hostname

    localhost.localdomain

SuSE:
-----
::

    vi /etc/HOSTNAME

    localhost.localdomain

19. Set ulimits in /etc/security/limits.conf
============================================
::

    vi /etc/security/limits.conf

    # /etc/security/limits.conf
    #
    #Each line describes a limit for a user in the form:
    #
    #<domain>        <type>  <item>  <value>
    #
    #Where:
    #<domain> can be:
    #        - an user name
    #        - a group name, with @group syntax
    #        - the wildcard *, for default entry
    #        - the wildcard %, can be also used with %group syntax,
    #                 for maxlogin limit
    #
    #<type> can have the two values:
    #        - "soft" for enforcing the soft limits
    #        - "hard" for enforcing hard limits
    #
    #<item> can be one of the following:
    #        - core - limits the core file size (KB)
    #        - data - max data size (KB)
    #        - fsize - maximum filesize (KB)
    #        - memlock - max locked-in-memory address space (KB)
    #        - nofile - max number of open files
    #        - rss - max resident set size (KB)
    #        - stack - max stack size (KB)
    #        - cpu - max CPU time (MIN)
    #        - nproc - max number of processes
    #        - as - address space limit (KB)
    #        - maxlogins - max number of logins for this user
    #        - maxsyslogins - max number of logins on the system
    #        - priority - the priority to run user process with
    #        - locks - max number of file locks the user can hold
    #        - sigpending - max number of pending signals
    #        - msgqueue - max memory used by POSIX message queues (bytes)
    #        - nice - max nice priority allowed to raise to values: [-20, 19]
    #        - rtprio - max realtime priority
    #
    #<domain>      <type>  <item>         <value>
    #

    *                soft    nofile          32768
    *                hard    nofile          32768
    hadoop           -       nofile          32768
    hadoop           -       nproc           unlimited

    #CDH 5 additional users
    hdfs             -       nofile          32768
    hbase            -       nofile          32768
    spark            soft    nofile          65535
    spark            hard    nofile          65535
    spark            -       nproc           32768

    # End of file

20. Configure NTP
=================

RedHat/CentOS/Oracle Linux/SuSE:
--------------------------------
::

    vi /etc/ntp.conf

    # For more information about this file, see the man pages
    # ntp.conf(5), ntp_acc(5), ntp_auth(5), ntp_clock(5), ntp_misc(5), ntp_mon(5).

    driftfile /var/lib/ntp/drift

    # Permit time synchronization with our time source, but do not
    # permit the source to query or modify the service on this system.
    restrict default kod nomodify notrap nopeer noquery
    restrict -6 default kod nomodify notrap nopeer noquery

    # Permit all access over the loopback interface.  This could
    # be tightened as well, but to do so would effect some of
    # the administrative functions.
    restrict 127.0.0.1
    restrict -6 ::1

    # Use public servers from the pool.ntp.org project.
    # Please consider joining the pool (http://www.pool.ntp.org/join.html).
    server corp.intel.com iburst
    server pool.ntp.org iburst

    # Undisciplined Local Clock. This is a fake driver intended for backup
    # and when no outside source of synchronized time is available.
    server 127.127.1.0     # local clock
    fudge  127.127.1.0 stratum 10

    includefile /etc/ntp/crypto/pw

    # Key file containing the keys and key identifiers used when operating
    # with symmetric key cryptography.
    keys /etc/ntp/keys

Ubuntu/Debian:
--------------
::

    vi /etc/ntp.conf

    # For more information about this file, see the man pages
    # ntp.conf(5), ntp_acc(5), ntp_auth(5), ntp_clock(5), ntp_misc(5), ntp_mon(5).

    driftfile /var/lib/ntp/drift

    # Permit time synchronization with our time source, but do not
    # permit the source to query or modify the service on this system.
    restrict default kod nomodify notrap nopeer noquery
    restrict -6 default kod nomodify notrap nopeer noquery

    # Permit all access over the loopback interface.  This could
    # be tightened as well, but to do so would effect some of
    # the administrative functions.
    restrict 127.0.0.1
    restrict -6 ::1

    # Use public servers from the pool.ntp.org project.
    # Please consider joining the pool (http://www.pool.ntp.org/join.html).
    server corp.intel.com iburst
    server pool.ntp.org iburst

    # Undisciplined Local Clock. This is a fake driver intended for backup
    # and when no outside source of synchronized time is available.
    server 127.127.1.0     # local clock
    fudge  127.127.1.0 stratum 10  

    includefile /etc/ntp/crypto/pw

    # Key file containing the keys and key identifiers used when operating
    # with symmetric key cryptography.
    keys /etc/ntp/keys

In RedHat/CentOS/Oracle Linux:
------------------------------
::

    service ntpd start
    chkconfig ntpd on

In Ubuntu/Debian:
-----------------
::

    /etc/init.d/ntp start
    chkconfig ntp on

In SuSE:
--------
::

    service ntp start
    chkconfig ntp on

21. Configure Pacific timezone
==============================
::

    cp /usr/share/zoneinfo/PST8PDT /etc/localtime

22. Turn off SSH root login
===========================
::

    sed -i '/PermitRootLogin/c\PermitRootLogin no' /etc/ssh/sshd_config
    /etc/init.d/sshd restart

23. Reboot!
===========
::

    reboot

