=========================
Best Known Methods (User)
=========================

.. contents:: Table of Contents
    :local:

-------------------------
Configuration information
-------------------------

Partitioning
============

Rules of thumb:
    | Choose a reasonable number of partitions: no smaller than 100, no larger than 10,000 (large cluster)
    | Lower bound: at least 2x number of cores in your cluster
    | Too few partitions results in:
    |    Less concurrency
    |    More susceptible to data skew
    |    Increased memory pressure
    | 
    |   Upper bound: ensure your tasks take at least 100ms (if they are going faster,
        then you are probably spending more time scheduling tasks than executing them)
    |   Too many partitions results in:
    |       Time wasted spent scheduling
    |       If you choose a number way too high then more time will be spent scheduling than executing
            10,000 would be way too big for a 4 node cluster

.. ifconfig:: internal_docs

    Notes:
        |   Graph builder could not complete 1GB Netflix graph with less than 60 partitions - about 90 was optimal
            (larger needed for large data)
        |   Graph builder ran into issues with partition size larger than 2000 on 4 node cluster with larger data sizes

------
Python
------

Server Connection
=================

Ping the server::

    >>> import intelanalytics as ia
    >>> ia.server.ping()
    Successful ping to Intel Analytics at http://localhost:9099/info

View and edit the server connection::

    >>> print ia.server
    host:    localhost
    port:    9099
    scheme:  http
    version: v1

    >>> ia.server.host
    'localhost'

    >>> ia.server.host = '10.54.99.99'
    >>> ia.server.port = None
    >>> print ia.server
    host:    10.54.99.99
    port:    None
    scheme:  http
    version: v1

Reset configuration back to defaults::

    >>> ia.server.reset()
    >>> print ia.server
    host:    localhost
    port:    9099
    scheme:  http
    version: v1

Errors
======

By default the toolkit does not print the full stack trace when exceptions occur.  To see the full python stack trace of the last (i.e. most recent) exception::

    >>> print ia.errors.last

To enable always printing the full python stack trace, set the 'show_details' property::

    >>> ia.errors.show_details = True

Tab Completion
==============

Allows you to use the tab key to complete your typing for you.

If you are running with a standard Python REPL (not iPython, bPython, or the like) you will have to set up the tab completion manually:

Create a .pythonrc file in your home directory with the following contents::

    import rlcompleter, readline
    readline.parse_and_bind('tab:complete')


Or you can just run the two lines in your REPL session.

This will let you do the tab completion, but will also remember your history over multiple sessions::

    # Add auto-completion and a stored history file of commands to your Python
    # interactive interpreter. Requires Python 2.0+, readline.

    import atexit
    import os
    import readline
    import rlcompleter
    import sys

    # Autocomplete is bound to the Esc key by default, so change it to tab.
    readline.parse_and_bind("tab: complete")

    historyPath = os.path.expanduser("~/.pyhistory")

    def save_history(historyPath=historyPath):
        import readline
        readline.write_history_file(historyPath)

    if os.path.exists(historyPath):
        readline.read_history_file(historyPath)

    atexit.register(save_history)

    # anything not deleted (sys and os) will remain in the interpreter session
    del atexit, readline, rlcompleter, save_history, historyPath

Note:
    If the .pythonrc does not take effect, add PYTHONSTARTUP in your .bashrc file::

        export PYTHONSTARTUP=~/.pythonrc

-----
Spark
-----

Spark Bug
=========

When implementing a plugin, using Spark prior to version 1.1.0, avoid using the Spark *top* function.
Instead, use the less efficient *sortByKey* function.
The Spark *top* function has a bug filed against it when using Kryo serializer.
This has been fixed in Spark 1.1.0.
There is a known work-around, but there are issues implementing it in our plugin architecture.
See https://issues.apache.org/jira/browse/SPARK-2306.


Resolving disk full issue while running Spark jobs
==================================================

If you are using a Red Hat cluster or an old CentOS cluster, due to the way the /tmp file system is setup, 
you might see that while running spark jobs, your /tmp drive becomes full and causes the jobs to fail.

This is because Spark and other CDH services, by default use /tmp as the temporary location to store files required during 
run time including but not limited to shuffle data.

In order to resolve this, follow these instructions:

1)  Stop the Intelanalytics service

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
    | http://spark.apache.org/docs/0.9.0/configuration.html
    | http://spark.apache.org/docs/0.9.0/tuning.html

Nice thread on how Shuffle works in Spark,
    http://apache-spark-user-list.1001560.n3.nabble.com/How-does-shuffle-work-in-spark-td584.html

