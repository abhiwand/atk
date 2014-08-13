================
Virtual Machines
================

Thank you for your interest in the Intel Analytics Toolkit 0.8 beta.
Please follow the steps below to download your copy of the beta product.

------------------------
VM Download Instructions
------------------------

The VM image is downloaded from AWS.
The download will require that you have the AWS Command Line Interface (CLI) client on your system.
Instructions for downloading and installing can be found at `Amazon cli documentation <http://docs.aws.amazon.com/cli/latest/userguide/installing.html]>`.

After installing the interface you can verify the installation by running::

    aws help

You should see man style help text for your output.
::

    AWS()



    NAME
        aws -

    DESCRIPTION
        The  AWS  Command  Line  Interface is a unified tool to manage your AWS
        services.

    SYNOPSIS
            aws [options] <command> <subcommand> [parameters]

        Use aws command help for information on a specific command.

    OPTIONS
            --debug (boolean)

        Turn on debug logging.


After installing run::

    aws configure

You will be prompted for to enter your access and secret keys you were given.
When Prompted for a Default region name use 'us-west-2'.
When prompted for default output format use 'json'.
::
    AWS Access Key ID [None]: my access key
    AWS Secret Access Key [None]: my secret key
    Default region name [None]: us-west-2
    Default output format [None]: json

To download the VM run::

    aws s3 cp s3://intel-analytics-repo/release/latest/vm/IntelAnalytics-0.8.0-CDH-5.0.3.tar.gz
    
Depending on the release you would like to download, you can change '0.8.0' to the latest release, or another you would like to try.

--------------------------------
Running Intel Analytics VM image
--------------------------------

To jump start usage of the IA toolkit we have modified the base Cloudera Virtual Box VM with the IA toolkit.
Currently the Intel Analytics toolkit VM only supports `Virtual Box <https://www.virtualbox.org/>`.
We will not cover the installation of Virtual Box.
Virtual Box supports many platforms and can be `downloaded <https://www.virtualbox.org/wiki/Downloads]>` for free.
The installation documentation is also `available online <https://www.virtualbox.org/manual/UserManual.html]>`.

Installation
============

After acquiring the VM extract the archive::

    tar -xvf IntelAnalytics-0.8.0-CDH-5.0.3.tar.gz

You should have two files after extracting::

    IntelAnalytics-0.8.0-CDH-5.0.3-disk1.vmdk
    IntelAnalytics-0.8.0-CDH-5.0.3.ovf

Import
======

In Virtual Box go to the file menu then import appliance.

File \-> Import Appliance

.. image:: ad_inst_vm_01.*

Select your file.

.. image:: ad_inst_vm_02.*

Import IA VM

.. image:: ad_inst_vm_03.*

After clicking 'Import' wait for the VM to be imported

.. image:: ad_inst_vm_04.*

Once the VM is imported go ahead and boot the VM by selecting the VM and clicking start

.. image:: ad_inst_vm_05.*

Before you start
================

After every reboot of the VM you must restart the IA server.
::

    sudo service intelanalytics restart

Examples
========

The VM is pre-configured and installed with IA toolkit.
It has many examples and datasets to get you started as soon as the VM is booted.
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

    cd /home/cloudera/examples
    python SCRIPT_NAME.py

where SCRIPT_NAME is any of the scripts in '/home/cloudera/example'.

Logs
====

If you need to debug changes to the scripts (or peak behind the curtain) the log files are located at '/var/log/intelanalytics/rest-server/output.log'.
::

    sudo tail -f /var/log/intelanalytics/rest-server/output.log

Updating
========

If you have been given access and secret keys, edit '/etc/yum.repos.d/ia.repo' and replace *myKey* and *mySecret*.
Now you will be able to run Yum commands to check for and do updates.
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

