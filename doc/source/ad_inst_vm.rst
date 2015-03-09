================
Virtual Machines
================

.. contents:: Table of Contents
    :local:

------------
Introduction
------------

This guide will go through the download and import of the |IAT| beta.
Currently the |IAT| VM only supports
`Virtual Box <https://www.virtualbox.org/>`_.
These instructions do not cover the installation of Virtual Box.
Virtual Box supports many platforms and can be `downloaded for free
<https://www.virtualbox.org/wiki/Downloads>`_.
The installation documentation is also
`available online <https://www.virtualbox.org/manual/UserManual.html>`_.

------------
Requirements
------------

#.  12GB of memory needs to be allocated to the VM
#.  45GB of free hard drive space
#.  Working Virtual Box 4.3 installation

-----------------
Download VM Image
-----------------

Note:
    Open a Linux shell (or for Windows user a command prompt) to run the
    various commands.

The VM image is downloaded from AWS.
The download will require the AWS Command Line Interface (CLI) client.
Instructions for downloading and installing CLI can be found at `Amazon cli
documentation <http://docs.aws.amazon.com/cli/latest/userguide/installing.html>`_.

After installing the interface it is possible to verify the installation by
running::

    aws --version

The result is similar to this::

    aws-cli/1.2.9 Python/2.7.6 Linux/3.8.0-35-generic

Take note of the aws-cli version, as it must be greater or equal to 1.2.9.
Older versions of the aws-cli client won't work with the restricted permissions.

If the aws-cli client is already installed, it can be updated with pip or
download the new windows MSI.
::

    sudo pip install -U awscli
 
After aws installation, run::

    aws configure

The program will prompt for the access and secret tokens given at registration.
When Prompted for the "Default region name" use "us-west-2".
When prompted for the "Default output format" use "json".
::

    AWS Access Key ID [None]: my access key
    AWS Secret Access Key [None]: my secret key
    Default region name [None]: us-west-2
    Default output format [None]: json

.. only:: html

    To download the VM run::

        aws s3 cp s3://intel-analytics-repo/release/latest/VM/IntelAnalytics-#.#.#-CDH-5.3.1.tar.gz
 
.. only:: latex

    To download the VM run::

        aws s3 cp s3://intel-analytics-repo/release/latest/VM/IntelAnalytics
            -#.#.#-CDH-5.3.1.tar.gz
      
    The preceding line was broken across multiple lines for improved viewing on
    various media.
    The line should be entered as one line with no gaps (spaces).

The part ``#.#.#`` must be changed to the desired release to download.
If the version of :abbr:`CDH (Cloudera Hadoop)` has changed, it is
necessary to change that as well.

---------------
Extract Archive
---------------

Extracting On Windows
=====================
Extracting on Windows is relatively easy.
Use `7zip <http://7-zip.org/>`_ (or equivalent tool) to extract the archive.

Extracting On Linux
===================
After acquiring the VM, extract the archive.
Replace ``#.#.#`` with the release number::

    tar -xvf IntelAnalytics-#.#.#-CDH-5.3.1.tar.gz

After extracting there should be two (2) files::

    IntelAnalytics-#.#.#-CDH-5.3.1-disk1.vmdk
    IntelAnalytics-#.#.#-CDH-5.3.1.ovf

------------
Import Image
------------
In Virtual Box go to the file menu then import appliance.

File -> Import Appliance

.. figure:: ad_inst_vm_01.*

Select the .ovf file extracted from the vm image earlier

.. figure:: ad_inst_vm_02.*
 
Import |IAT| VM

.. figure:: ad_inst_vm_03.*
 
After clicking 'Import' wait for the VM to be imported

.. figure:: ad_inst_vm_04.*
 
Once the VM is imported go ahead and boot the VM by selecting the VM and
clicking *start*

.. figure:: ad_inst_vm_05.*
 
---------------------
Running |IA| VM image
---------------------

Before starting
===============

After every reboot of the VM, the |IAT| server must also be restarted.
::

    sudo service intelanalytics restart

Upon restart, if the service wasn't running before it was told to stop,
the system will report::
    
    initctl: Unknown instance:

This message can be safely ignored.


Using Sample Scripts
====================

The VM is pre-configured and installed with the |IAT|.
It has several examples and datasets to get started as soon as the VM is booted.

The examples are located in '/home/cloudera/examples'.
::

    drwxr-xr-x 2 cloudera cloudera 4096 Aug  1 00:53 datasets
    -rw-r--r-- 1 cloudera cloudera 1100 Aug  1 10:15 lbp.py
    -rw-r--r-- 1 cloudera cloudera  707 Aug  1 00:53 lda.py
    -rw-r--r-- 1 cloudera cloudera  930 Aug  1 00:53 lp.py

The datasets are located in '/home/cloudera/examples/datasets' and
'hdfs://user/iauser/datasets/'.
::

    -rw-r--r--   1 iauser iauser        122 2014-08-01 /user/iauser/datasets/README
    -rw-r--r--   1 iauser iauser     617816 2014-08-01 /user/iauser/datasets/apl.csv
    -rw-r--r--   1 iauser iauser    8162836 2014-08-01 /user/iauser/datasets/lbp_edge.csv
    -rw-r--r--   1 iauser iauser     188470 2014-08-01 /user/iauser/datasets/lp_edge.csv
    -rw-r--r--   1 iauser iauser  311641390 2014-08-01 /user/iauser/datasets/test_lda.csv

The datasets in '/home/cloudera/examples/datasets' are for reference.
The actual data that is being used by the Python examples and the |IAT| server
is in 'hdfs://user/iauser/datasets'.

To run any of the Python example scripts, start in the examples directory and
start Python with the script name::

    python <SCRIPT_NAME>.py

where ``<SCRIPT_NAME>`` is any of the scripts in '/home/cloudera/example'.

Example::

    cd /home/cloudera/examples
    python pr.py

Logs
====

To debug changes to the scripts (or peak behind the curtain) the log files are
located at '/var/log/intelanalytics/rest-server/output.log'.
To show the log information as it gets appended to the the log file run ``tail
-f``::

    sudo tail -f /var/log/intelanalytics/rest-server/output.log
    
More details about the logs can be found here: :doc:`ad_log`.

.. toctree::
    :hidden:
    
    ad_log
    
Updating
========

Upon receipt of access and secret tokens, edit '/etc/yum.repos.d/ia.repo' and
replace *myKey* and *mySecret*.
Afterwards, it is recommended to run ``yum`` commands to check for and perform
updates.

.. only:: html

    ::

        sudo [vi/vim] /etc/yum.repos.d/ia.repo

        [Intel Analytics repo]
        name=Intel Analytics yum repo
        baseurl=https://s3-us-west-2.amazonaws.com/intel-analytics-repo/release/#.#.#/yum/dists/rhel/6
        gpgcheck=0
        priority=1
        #enabled=0
        s3_enabled=0
        key_id=myKey
        secret_key=mySecret

.. only:: latex

    ::

        sudo [vi/vim] /etc/yum.repos.d/ia.repo

        [Intel Analytics repo]
        name=Intel Analytics yum repo
        baseurl=https://s3-us-west-2.amazonaws.com/intel-analytics-repo/
            release/#.#.#/yum/dists/rhel/6
        gpgcheck=0
        priority=1
        #enabled=0
        s3_enabled=0
        key_id=myKey
        secret_key=mySecret

Replace the ``#.#.#`` with the correct release number.
To check for new updates and see the difference between the new and installed
version::

    sudo yum info intelanalytics-rest-server

To update::

    sudo yum update intelanalytics-rest-server

