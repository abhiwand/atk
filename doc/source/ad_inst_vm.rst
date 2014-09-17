================
Virtual Machines
================

.. contents:: Table of Contents
    :local:

------------
Introduction
------------

Thank you for your interest in the Intel Analytics Toolkit 0.8 beta.
This guide will walk you through the download and import of the Intel Analytics beta.
Currently the Intel Analytics toolkit VM only supports `Virtual Box <https://www.virtualbox.org/>`_.
We will not cover the installation of Virtual Box.
Virtual Box supports many platforms and can be `downloaded for free <https://www.virtualbox.org/wiki/Downloads>`_.
The installation documentation is also
`available online <https://www.virtualbox.org/manual/UserManual.html>`_.

------------
Requirements
------------

A)  12GB of memory needs to be allocated to the VM

#)  30 GB of free hard drive space

#)  Working Virtual Box installation 4.3

-----------------
Download VM Image
-----------------

Note:
    Open a Linux shell (or for Windows user a command prompt) to run the various commands.

.. figure:: ad_inst_vm_00.*

The VM image is downloaded from AWS.
The download will require that you have the AWS Command Line Interface (CLI) client on your system.
Instructions for downloading and installing can be found at `Amazon cli documentation <http://docs.aws.amazon.com/cli/latest/userguide/installing.html>`_.

After installing the interface you can verify the installation by running::

    aws --version

You should see this kind of response::

    aws-cli/1.2.9 Python/2.7.6 Linux/3.8.0-35-generic

Take note of the aws-cli version.
Make sure the version is greater or equal to 1.2.9.
Older versions of the aws-cli client won't work with the restricted permissions.

If you happen to have the aws-cli client you can update the client with pip.
::

    expand source
 
After aws installation, run::

    aws configure

You will be prompted to enter the access and secret keys you were given.
When Prompted for the "Default region name" use "us-west-2".
When prompted for the "Default output format" use "json".
::

    AWS Access Key ID [None]: my access key
    AWS Secret Access Key [None]: my secret key
    Default region name [None]: us-west-2
    Default output format [None]: json

To download the VM run::

    aws s3 cp s3://intel-analytics-repo/release/latest/VM/IntelAnalytics-0.8.5-CDH-5.1.0.tar.gz
    
Depending on the release you would like to download, you can change '0.8.0' to the latest release, or another you would like to try.

---------------
Extract Archive
---------------

Extracting On Windows
=====================
Extracting on Windows is relatively easy.
Use any of the following tools to extract the archive, `7zip <http://7-zip.org/>`_ ,
`WinZip <http://winzip.com/>`_ , `Winrar <http://rarlab.com/>`_, etc...

Extracting On Linux
===================
After acquiring the VM, extract the archive::

    tar -xvf IntelAnalytics-0.8.5-CDH-5.1.0.tar.gz

You should have two files after extracting::

    IntelAnalytics-0.8.5-CDH-5.1.0-disk1.vmdk
    IntelAnalytics-0.8.5-CDH-5.1.0.ovf

------------
Import Image
------------
In Virtual Box go to the file menu then import appliance.

File -> Import Appliance

.. figure:: ad_inst_vm_01.*

Select your .ovf file extracted from the vm image earlier

.. figure:: ad_inst_vm_02.*
 
Import Intel Analytics VM

.. figure:: ad_inst_vm_03.*
 
After clicking 'Import' wait for the VM to be imported

.. figure:: ad_inst_vm_04.*
 
Once the VM is imported go ahead and boot the VM by selecting the VM and clicking start

.. figure:: ad_inst_vm_05.*
 
--------------------------------
Running Intel Analytics VM image
--------------------------------

Before you start
================

After every reboot of the VM you must restart the |IA| server.
::

    sudo service intelanalytics restart

If you restart service and you see the following output you can ignore it.
All it means is that the service wasn't running before it was told to stop.
::
    
    initctl: Unknown instance:

Using Sample Scripts
====================

The VM is pre-configured and installed with |IA| toolkit.
It has several examples and datasets to get you started as soon as the VM is booted.

The examples are located in '/home/cloudera/examples'.
::

    drwxr-xr-x 2 cloudera cloudera 4096 Aug  1 00:53 datasets
    -rw-r--r-- 1 cloudera cloudera 1100 Aug  1 10:15 lbp.py
    -rw-r--r-- 1 cloudera cloudera  707 Aug  1 00:53 lda.py
    -rw-r--r-- 1 cloudera cloudera  930 Aug  1 00:53 lp.py

The datasets are located in '/home/cloudera/examples/datasets' and 'hdfs://user/iauser/datasets/'.
::

    -rw-r--r--   1 iauser iauser        122 2014-08-01 00:53 /user/iauser/datasets/README
    -rw-r--r--   1 iauser iauser     617816 2014-08-01 00:53 /user/iauser/datasets/apl.csv
    -rw-r--r--   1 iauser iauser    8162836 2014-08-01 00:53 /user/iauser/datasets/lbp_edge.csv
    -rw-r--r--   1 iauser iauser     188470 2014-08-01 00:53 /user/iauser/datasets/lp_edge.csv
    -rw-r--r--   1 iauser iauser  311641390 2014-08-01 00:53 /user/iauser/datasets/test_lda.csv

The datasets in '/home/cloudera/examples/datasets' are for reference,
the actual data that is being used by the python examples and the intelanalytics server is in 'hdfs://user/iauser/datasets'.

To run any of the python example scripts, make sure you are in the examples directory and start python with the script name::

    python <SCRIPT_NAME>.py

where ``<SCRIPT_NAME>`` is any of the scripts in '/home/cloudera/example'.

Make sure you are in the examples directory first, then run the desired script:

Example::

    cd /home/cloudera/examples
    python pr.py

Logs
====

If you need to debug changes to the scripts (or peak behind the curtain) the log files are located at '/var/log/intelanalytics/rest-server/output.log'.
To show the log information as it gets appended to the the log file run "tail -f"::

    sudo tail -f /var/log/intelanalytics/rest-server/output.log
    
More details about the logs can be found here: :doc:`ad_log`.

.. toctree::
    :hidden:
    
    ad_log
    
Updating
========

If you have been given access and secret keys, edit '/etc/yum.repos.d/ia.repo' and replace *myKey* and *mySecret*.
Afterwards you will be able to run ``yum`` commands to check for and do updates.
::

    sudo [vi/vim] /etc/yum.repos.d/ia.repo

    [Intel Analytics repo]
    name=Intel Analytics yum repo
    baseurl=https://s3-us-west-2.amazonaws.com/intel-analytics-repo/release/0.8.0/yum/dists/rhel/6
    gpgcheck=0
    priority=1
    #enabled=0
    s3_enabled=0
    key_id=myKey
    secret_key=mySecret

To check for new updates and see the difference between the new and installed version::

    sudo yum info intelanalytics-rest-server

To update::

    sudo yum update intelanalytics-rest-server

.. |IA| replace:: Intel Analytics
