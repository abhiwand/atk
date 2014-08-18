===============================
Cloudera Hadoop 5 Configuration
===============================

This guide will walk you through the process of configuring Cloudera Hadoop 5 on a physical or virtual cluster.

------------------------
Install Cloudera Manager
------------------------
Download and install the `Cloudera Manager`_.

-------------------------------------------------
Set Proxy Info in Cloudera Manager (if necessary)
-------------------------------------------------

.. ifconfig:: internal_docs

    The first step to take in the Cloudera Manager web interface is to add JF's proxy information.

    1. Click the "Cloudera Manager" hyperlink graphic on the top left portion of the window.
    #. Click the Administration drop-down along the top of the window, then select Settings.
    #. Select the Network button long the menu pane to the left
    #. In the Proxy Server field, enter proxy.jf.intel.com
    #. In the Proxy Port field, enter 911
    #. Hit the Save Changes button to the top right of the active menu
    #. Hit the admin drop-down menu at the top right corner of the window and logout
    #. Log back in using the same admin admin username password combo

.. ifconfig:: internal_docs == False

    The first step to take in the Cloudera Manager web interface is to add your proxy information.

    1. Click the "Cloudera Manager" hyperlink graphic on the top left portion of the window.
    #. Click the Administration drop-down along the top of the window, then select Settings.
    #. Select the Network button long the menu pane to the left
    #. In the Proxy Server field, enter the proxy qualified name, for example, "proxy.my.company.com"
    #. In the Proxy Port field, enter your proxy port number
    #. Hit the Save Changes button to the top right of the active menu
    #. Hit the admin drop-down menu at the top right corner of the window and logout
    #. Log back in using the same admin admin username password combo

-------------------
Submit License File
-------------------

.. ifconfig:: internal_docs

    To complete this step, you must first acquire the "intel_bda_graph_analytics_lab_machines_dev_cloudera_enterprise_license.txt" file,
    which can be found in the "licenses" folder of either the build server or the CLC.

    1. Under the Cloudera Enterprise column, click on the empty text field to the left of the Upload button
    #. Select the license file
    #. Hit the Upload button
    #. Hit Continue on the bottom right of the window  

.. ifconfig:: internal_docs == False

    To complete this step, you must first acquire the Cloudera license file.

    1. Under the Cloudera Enterprise column, click on the empty text field to the left of the Upload button
    #. Select the license file
    #. Hit the Upload button
    #. Hit Continue on the bottom right of the window  

----------------
Specifying Hosts
----------------

This step connects your master node to the rest of the cluster.
The syntax used to search for hostnames is identical to what you will find in the /etc/hosts file or by DNS lookup.

Hit Continue through the "Thank you for choosing Cloudera Manager and CDH" window.
In the text field, enter the hostnames of each node in the following syntax::
For example::

    master.clustername.cluster
    node[01-03].clustername.cluster

Where ``clustername`` is the name of your cluster,
and ``[01-03]`` is the range of slave nodes in your cluster (``[01-07]`` for an 8 node cluster,
``[01-15]`` for a 16 node cluster).

Hit Search and make sure that you detect as many hosts as there are nodes in your cluster.
Check below images for examples.
If all correct hosts are selected, hit Continue.
Otherwise, click New Search. 

.. image:: ad_inst_cloudera_04.*
   :width: 80%
   :align: center

----------------------------
Select CDH Parcel Repository
----------------------------

At this point, you must complete step 3 to continue if you have not yet already.

In Custom Repository field (NOT "Custom Repository" button drop-down), enter::

    http://archive.cloudera.com/cdh5/parcels/latest

Hit the Add button to the right of the text field.
Wait around a minute then hit the Continue button when it lights up.

.. image:: ad_inst_cloudera_05.*
   :width: 80%
   :align: center

------------------------------
Choose Java encryption setting
------------------------------
Java encryption is not currently supported.

----------------------------- 
Provide SSH Login Credentials
----------------------------- 
Fill out appropriate login information for CDH administrator user.

--------------------------------------
Wait Through Installation on All Nodes
--------------------------------------
The next couple of windows are just progress bars.
If any of them fail and turn red, sometimes just hitting "Retry" will fix the problem nodes.

Hit Continue button when it lights up after the progress bar fills.
You will be greeted by more progress bars.
Wait and hit Continue when they finish too.   

.. image:: ad_inst_cloudera_07.*
   :width: 80%
   :align: center

--------------------------------------
Inspect Hosts for proper configuration
--------------------------------------
Check that the host inspector doesn't throw any critical errors at you.
Take note of anything else that doesn't have a green check mark next to it and resolve issue.

Click Finish

.. image:: ad_inst_cloudera_08.*
   :width: 80%
   :align: center

--------------------------------------------------- 
Choose the CDH5 Services to Install On Your Cluster
--------------------------------------------------- 

The following windows will bring you through the process of installing services and roles on each node in the cluster.
This is our default setup.

In the "Choose a combination of services to install" dialogue, select the "Custom Services" button.
In the drop-down menu, mark the following boxes:

* HBase
* HDFS
* Oozie
* Spark
* Sqoop 2
* YARN (MR2 Included)
* ZooKeeper

Click Continue.                

.. image:: ad_inst_cloudera_09.*
   :width: 80%
   :align: center

--------------------------
Customize Role Assignments
--------------------------

This page allows you to designate which roles your different nodes will take up.
In a default loadout, almost all of these fields will be left to their default, but there are four that need to be changed.

1. Under the HBase section, click on the HBase Thrift Server dialogue and select master node of your cluster
#. Under the HDFS section, click on the Secondary Name Node dialogue and select node01 of your cluster
#. Under the YARN section, click on the JobHistory Server dialogue and select node01 of your cluster
#. Under the ZooKeeper section, click on the Server dialogue and select node01, node02, node03 of your cluster

Leave all other fields in their default values and click Continue.

Changes to make near the top:

.. image:: ad_inst_cloudera_10a.*
   :width: 80%
   :align: center
 

Changes to make near the bottom:

.. image:: ad_inst_cloudera_10b.*
   :width: 80%
   :align: center
 
-------------- 
Database Setup
-------------- 

The "Database Host Name" field should auto-populate with the hostname of the system on which cloudera manager is installed.
If not, fill that in.

Click Test Connection.
If successful, click Continue.

.. image:: ad_inst_cloudera_11.*
   :width: 80%
   :align: center
 
-------------- 
Review Changes
-------------- 

In this window, all fields should remain their default values.

Click Continue.

--------------------------------
Finishing Up in Cloudera Manager
--------------------------------

The next page requires no interaction. Just more loading bars.

1. Wait for all services to start up, then hit Continue
#. In the "Congratulations!" window, click Finish
#. Some of the health indicators may be orange or red in the first few moments of the cluster's life.
   Wait a minute for them to all turn green.
#. In the Cloudera Manager page, change the name of the cluster by hitting the drop down arrow to the right of the Cluster 1 heading
   then clicking Rename Cluster
#. In the Cloudera Manager, hit the admin drop-down at the top right corner of the screen and select Change Password.
   Change the password as desired

.. image:: ad_inst_cloudera_13.*
   :width: 50%
   :align: center
 
------------------------ 
Final Settings and Tests
------------------------ 
Test functionality of hdfs.

------
Tweaks
------

The graph machine learning algorithms in our toolkit use the Giraph graph-processing framework.
Giraph is designed to run the whole graph computation in memory, and requires large amounts of memory to process big graphs.
We recommend at least 4GB of memory per map task to cater for graphs with supernodes.
Giraph jobs are scheduled using Yarn.
If a Giraph job requests twice the amount of memory configured in Yarn, then the Yarn resource manager will not schedule it causing the job to hang.

To run Giraph jobs, ensure that the memory settings in CDH match those in application.conf using one of the following approaches: 

1.  Modify the following Yarn configuration in CDH to match the setting under intel.analytics.giraph in application.conf.
    Under the Yarn section in CDH, click on “Configuration” and select “View and Edit”.

    a.  Search for “mapreduce.map.memory.mb” in the search box on the upper left corner.
        Modify “mapreduce.map.memory.mb” to match mapreduce.map.memory.mb in application.conf (currently 8192 MB)
    #.  Search for “mapreduce.map.java.opts.max” in the search box.
        Modify this setting to match “mapreduce.map.java.opts” in the application.conf (currently 6554MB).
        The rule of thumb is that mapreduce.map.java.opts.max should be at most 0.85* mapreduce.map.memory.mb
    #.  Save these changes.
    #.  Click on “Actions”, on the top-right corner and then “Deploy Client Configuration” to update the configurations across the cluster.
    #.  Restart Yarn.

#.  Limit the Giraph memory allocation in application.conf to match the configured CDH settings in Yarn.
    The relevant settings in our application.conf file are in intel.analytics.giraph:

    a.  mapreduce.map.memory.mb. This setting should match mapreduce.map.memory.mb in Yarn.
    #.  mapreduce.map.java.opts. This setting should match mapreduce.map.java.opts.max in Yarn.

| 

<- :doc:`ad_inst_pre_cloudera`
|-------------------------------|
:doc:`ad_inst_IA` ->

<- :doc:`index`

.. _Cloudera Manager: http://www.cloudera.com/content/support/en/downloads/cloudera_manager/cm-5-1-0.html
