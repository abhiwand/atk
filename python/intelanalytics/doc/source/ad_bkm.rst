==================
Best Known Methods
==================

---
Yum
---

If you have trouble downloading any of the dependencies run::

    yum clean all

or::

    yum clean expire-cache
    
-------------------------
Configuration information
-------------------------

Partitioning
============

Rules of thumb
    Choose a reasonable number of partitions: no smaller than 100, no larger than 10,000 (large cluster)
        Lower bound: at least 2x number of cores in your cluster
            Too few partitions results in
                Less concurrency
                More susceptible to data skew
                Increased memory pressure

        Upper bound: ensure your tasks take at least 100ms (if they are going faster, then you are probably spending more time scheduling tasks than
executing them)
            Too many partitions results in
                Time wasted spent scheduling
                If you choose a number way too high then more time will be spent scheduling than executing

            10,000 would be way too big for a 4 node cluster

        Notes

.. ifconfig:: internal_docs

            Graph builder could not complete 1GB Netflix graph with less than 60 partitions - about 90 was optimal (larger needed for large data)
            Graph builder ran into issues with partition size larger than 2000 on 4 node cluster with larger data sizes


-----
Spark
-----

Plug-in Spark Bug
=================

When implementing a plug-in, using Spark prior to version 1.1.0, avoid using the Spark *top* function.
Instead, use the less efficient *sortByKey* function.
The Spark *top* function has a bug filed against it when using Kryo serializer.
This has been fixed in Spark 1.1.0.
There is a known work-around, but there are issues implementing it in our plug-in architecture.
See https://issues.apache.org/jira/browse/SPARK-2306.

Resolving disk full issue while running Spark jobs
==================================================

If you are using a Red Hat cluster or an old CentOS cluster, due to the way the /tmp file system is setup,
you might see that while running spark jobs, your /tmp drive becomes full and causes the jobs to fail.

This is because Spark and other CDH services, by default use /tmp as the temporary location to store files required during
run time including but not limited to shuffle data.

In order to resolve this, follow these instructions:

1)  Stop the intelanalytics service

#)  From CDH Web UI: first stop "Cloudera Management Service", and then stop the CDH.

#)  Now run the following steps on each node:

    a)  Find your largest partition by running the command::

            df -h

    #)  Assuming /mnt is your largest partition, create the folder "/mnt/.bda/tmp", if it isn't already present::

            sudo mkdir -p /mnt/.bda/tmp

    #)  Set the permissions on this directory so that it's wide open::

            sudo chmod 1777 /mnt/.bda/tmp

    #)  Add the following line to your /etc/fstab file and save it::

            /mnt/.bda/tmp    /tmp    none   bind   0   0

    #)  Reboot the machine

#)  After all the nodes are rebooted, from CDH Web UI: first stop "Cloudera Management Service", and then stop the CDH.

Spark space concerns
====================
Whenever you run a Spark application, jars and logs go to /va/run/spark/work (or other location if configured in Cloudera Manager).
These can use up a bit of space eventually (over 140MB per command).

* Short-term workaround: periodically delete these files
* Long-term fix: Spark 1.0 will automatically clean up the files

----------
References
----------

Spark Docs
    http://spark.apache.org/docs/0.9.0/configuration.htmlhttps://securewiki.ith.intel.com/images/icons/linkext7.gif
    http://spark.apache.org/docs/0.9.0/tuning.htmlhttps://securewiki.ith.intel.com/images/icons/linkext7.gif

Nice thread on how Shuffle works in Spark,
    http://apache-spark-user-list.1001560.n3.nabble.com/How-does-shuffle-work-in-spark-td584.html


| 

<- :doc:`ad_inst`
<------------------------------>
:doc:`index` ->
