===============================
Cloudera Hadoop 5 Configuration
===============================

.. contents:: Table of Contents
    :local:

This guide discusses the process of configuring Cloudera Hadoop 5
on a physical or virtual cluster.

------------------------
Install Cloudera Manager
------------------------
The `Cloudera Manager`_ must be downloaded and installed.
For instructions, see the Cloudera website
(http://www.cloudera.com/content/support/en/downloads/cloudera_manager/cm-5-1-0.html).

-----------------------------------------
Proxy and Parcel Info in Cloudera Manager
-----------------------------------------

1.  On a web browser, go to the Cloudera Manager.
#.  Click the *Cloudera Manager* hyperlink graphic on the top left portion of
    the window
#.  Click the *Administration* drop-down along the top of the window, then
    select *Settings*
#.  Select the *Network* button along the menu pane to the left
#.  In the *Proxy Server* field, enter the proxy qualified name, for example,
    ``proxy.my.company.com``
#.  In the *Proxy Port* field, enter the proxy port number
#.  Select the *Parcels* button along the menu pane to the left

    a.  Overwrite the field that says
        ``http://archive.cloudera.com/cdh5/parcels/latest/`` with
        ``http//archive.cloudera.com/cdh5/parcels/5.3.1/``

#.  Hit the *Save Changes* button to the top right of the active menu
#.  Hit the admin drop-down menu at the top right corner of the window and
    logout
#.  Log back in using the same admin username password combo

-------------------
Submit License File
-------------------

1.  Acquire the Cloudera license file.
#.  Under the *Cloudera Enterprise* column, click on the empty text field to
    the left of the Upload button
#.  Select the license file
#.  Hit the *Upload* button
#.  Hit *Continue* on the bottom right of the window  

----------------
Specifying Hosts
----------------

This step connects the master node to the rest of the cluster.
The syntax used to search for hostnames is identical to what can be found in
the ``/etc/hosts`` file or by |DNS| lookup.

Hit *Continue* through the "Thank you for choosing Cloudera Manager and |CDH|"
window.
In the text field presented, enter the hostnames of each node in the following
syntax::

    master.clustername.cluster
    node[01-03].clustername.cluster

Where ``clustername`` is the name of the cluster, and ``[01-03]`` is the range
of slave nodes in the cluster (``[01-07]`` for an 8 node cluster,
``[01-15]`` for a 16 node cluster).

Hit *Search*, and make sure that the computer detects as many hosts as there
are nodes in the cluster.
See :ref:`ad_inst_cloudera_04` for examples.
If all correct hosts are selected, hit *Continue*.
Otherwise, click *New Search*. 

.. _ad_inst_cloudera_04:

.. figure:: ad_inst_cloudera_04.*
    :width: 60%
    :align: center

    Figure 1

-----------------------
|CDH| Parcel Repository
-----------------------

The repository/proxy information should populate the parcel list in a minute.
If not, click on *More Options* field to reconfigure.
Make sure ``CDH-5.3.1-1.cdh5.3.1.p).3`` is selected under *Remote Parcel
Repository* :ref:`ad_inst_cloudera_05` and then hit *Continue*.

.. _ad_inst_cloudera_05:

.. figure:: ad_inst_cloudera_05.*
    :width: 60%
    :align: center

    Figure 2

---------------
Java Encryption
---------------
Java encryption is not currently supported.

---------------------
SSH Login Credentials
---------------------
Fill out appropriate login information for |CDH| administrator user.

--------------------
Cluster Installation
--------------------
The next couple of windows are just progress bars.
If any of them fail and turn red, sometimes just hitting *Retry* will fix the
problem nodes. See :ref:`ad_inst_cloudera_07`.

Hit *Continue* button when it lights up after the progress bar fills.
You will be greeted by more progress bars.
Wait and hit *Continue* when they finish too.   

.. _ad_inst_cloudera_07:

.. figure:: ad_inst_cloudera_07.*
    :width: 60%
    :align: center

    Figure 3

------------------
Host Configuration
------------------
When the cluster installation finishes, look for any critical errors.
Take note of anything that doesn't have a green check mark next to it and
resolve the issue. See :ref:`ad_inst_cloudera_08`.

Click *Finish*

.. _ad_inst_cloudera_08:

.. figure:: ad_inst_cloudera_08.*
    :width: 60%
    :align: center

    Figure 4

-------------------------
|CDH| Services to Install
-------------------------

Choose the |CDH| 5 services to install on your cluster.
The following windows will show the process of installing services
and roles on each node in the cluster.
This is the |IAT| default setup.

In the "Choose a combination of services to install" dialogue, select the
"Custom Services" button.
In the drop-down menu, mark the following boxes:

* HBase
* HDFS
* Spark
* YARN (MR2 Included)
* ZooKeeper

See :ref:`ad_inst_cloudera_09`.
Click *Continue*.                

.. _ad_inst_cloudera_09:

.. figure:: ad_inst_cloudera_09.*
    :width: 60%
    :align: center

    Figure 5

--------------------------
Customize Role Assignments
--------------------------

This page allows designation of which roles the different nodes will take up.
In a default loadout, almost all of these fields will be left to their default,
but there are four that need to be changed.

#.  Under the HBase section, click on the *HBase Thrift Server* dialogue and
    select the "master" node of the cluster
#.  Under the |HDFS| section, click on the *Secondary Name Node* dialogue and
    select "node01" of the cluster
#.  Under the *YARN* section, click on the *Job History Server* dialogue and
    select "node01" of the cluster
#.  Under the *ZooKeeper* section, click on the *Server* dialogue and select
    "node01", "node02" and "node03" of the cluster

Leave all other fields in their default values and click *Continue*.

Changes to make near the top:

.. figure:: ad_inst_cloudera_10a.*
    :width: 60%
    :align: center
 
    Figure 6

Changes to make near the bottom:

.. figure:: ad_inst_cloudera_10b.*
    :width: 40%
    :align: center

    Figure 7
 
-------------- 
Database Setup
-------------- 

The "Database Host Name" field should auto-populate with the hostname of the
system on which Cloudera Manager is installed.
If not, fill that in.

Click *Test Connection*.
See :ref:`ad_inst_cloudera_11`.
If successful, click *Continue*.

.. _ad_inst_cloudera_11:

.. figure:: ad_inst_cloudera_11.*
    :width: 60%
    :align: center

    Figure 8
 
-------------- 
Review Changes
-------------- 

In the "Review Changes" window, all fields should remain their default values.

Click *Continue*.

--------------------------------
Finishing Up In Cloudera Manager
--------------------------------

The next page requires no interaction. Just more loading bars.

#.  Wait for all services to start up, then hit *Continue*.
#.  In the *Congratulations!* window, click *Finish*.
#.  Some of the health indicators may be orange or red in the first few moments
    of the cluster's life.
    Wait a minute for them to all turn green.
#.  In the Cloudera Manager page, change the name of the cluster by hitting the
    drop down arrow to the right of the *Cluster 1* heading then clicking
    *Rename Cluster*.
    See :ref:`ad_inst_cloudera_13`.
#.  In the Cloudera Manager, hit the admin drop-down at the top right corner of
    the screen and select *Change Password*.
    Change the password as desired.
#.  Select the Spark service from the homescreen.

    #.  Select *Configuration* along the top Spark menu.
    #.  Select *Worker Default Group* along the left side menu pane.
    #.  Select the *Work Directory* field and change the value to a directory
        with the capacity to store lots of temporaty data (the /mnt directory
        for virtual clusers).

.. _ad_inst_cloudera_13:

.. figure:: ad_inst_cloudera_13.*
    :width: 40%
    :align: center
 
    Figure 9

------------------------ 
Final Settings and Tests
------------------------ 
Test functionality of |HDFS|.

------
Tweaks
------

The graph machine learning algorithms in the |IAT| use the Giraph
graph-processing framework.
Giraph is designed to run the whole graph computation in memory, and requires
large amounts of memory to process big graphs.
There should be at least 4GB of memory per map task to cater for graphs with
supernodes.
Giraph jobs are scheduled using YARN.
If a Giraph job requests twice the amount of memory configured in YARN, then
the YARN resource manager will not schedule it causing the job to hang.

To run Giraph jobs, ensure that the memory settings in |CDH| match those in
application.conf using one of the following approaches: 

#.  Modify the following YARN configuration in |CDH| to match the setting under
    intel.analytics.giraph in application.conf.
    Under the YARN section in |CDH|, click on *Configuration* and select *View
    and Edit*.

    #.  Search for ``mapreduce.map.memory.mb`` in the search box on the upper
        left corner.
        Modify ``mapreduce.map.memory.mb`` to match mapreduce.map.memory.mb in
        application.conf (currently 8192 MB)
    #.  Search for ``mapreduce.map.java.opts.max`` in the search box.
        Modify this setting to match mapreduce.map.java.opts in
        application.conf (currently 6554MB).
        The rule of thumb is that mapreduce.map.java.opts.max should be at most
        85% of mapreduce.map.memory.mb
    #.  Search for ``yarn.nodemanager.resource.memory-mb`` in the search box. 
        Modify this setting to a multiple of ``mapreduce.map.memory.mb``.
        For example, to run at most 4 mappers on each node, and
        ``mapreduce.map.memory.mb`` is set to 8192MB, then set
        ``yarn.nodemanager.resource.memory-mb`` to 32768MB.
    #.  Save these changes.
    #.  Click on *Actions*, on the top-right corner and then *Deploy Client
        Configuration* to update the configurations across the cluster.
    #.  Restart YARN.

#.  Limit the Giraph memory allocation in application.conf to match the
    configured |CDH| settings in YARN.
    The relevant settings in the |IAT| application.conf file are in
    intel.analytics.giraph:

    #.  mapreduce.map.memory.mb.
        This setting should match mapreduce.map.memory.mb in YARN.
    #.  mapreduce.map.java.opts.
        This setting should match mapreduce.map.java.opts.max in YARN.
    #.  giraph.maxWorkers.
        The maximum value for this setting should be the maximum number of map
        tasks that can run on the cluster - 1.
        One mapper is reserved for the Giraph master, while the rest of the
        mappers are Giraph workers.
        Since Giraph is memory-intensive, a good estimate for giraph.maxWorkers
        is ((``Number of Yarn node managers`` *
        ``yarn.nodemanager.resource.memory-mb`` /
        ``yarn.nodemanager.resource.memory-mb``)-1).


