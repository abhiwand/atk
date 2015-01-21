==========================
Best Known Methods (Admin)
==========================

.. contents:: Table of Contents
    :local:

.. toctree::
    :hidden:

    ad_gitune
    ad_hbtune
    ad_partit
    ad_how.rst

-------------------------
Configuration information
-------------------------

.. include:: ad_gitune.rst
.. include:: ad_hbtune.rst
.. include:: ad_partit.rst

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

This is because Spark and other :abbr:`CDH (Cloudera Hadoop)` services, by default use /tmp as the temporary location to store files required during
run time including but not limited to shuffle data.

In order to resolve this, follow these instructions:

1)  Stop the intelanalytics service

#)  From :abbr:`CDH (Cloudera Hadoop)` Web UI: first stop "Cloudera Management Service", and then stop the :abbr:`CDH (Cloudera Hadoop)`.

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

#)  After all the nodes are rebooted, from :abbr:`CDH (Cloudera Hadoop)` Web UI: first stop "Cloudera Management Service", and then stop the :abbr:`CDH (Cloudera Hadoop)`.

Spark space concerns
====================
Whenever you run a Spark application, redundant jars and logs go to /va/run/spark/work (or other location
if configured in Cloudera Manager).
These can use up a bit of space eventually (over 140MB per command).

* Short-term workarounds\:

    *   Periodically delete these files
    *   Create a cron job to delete these files on a periodic basis.
        An example where the files are deleted eveyday at 2 am is::

            00 02 * * * sudo rm -rf /var/run/spark/work/app*

* Long-term fix\:

    *   Spark 1.0 will automatically clean up the files

.. include:: ad_how.rst

----------
References
----------

`Spark Docs <https://spark.apache.org/documentation.html>`__

Nice thread on how Shuffle works in Spark:
    http://apache-spark-user-list.1001560.n3.nabble.com/How-does-shuffle-work-in-spark-td584.html
