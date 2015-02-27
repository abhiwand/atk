=========================
Best Known Methods (User)
=========================

.. contents:: Table of Contents
    :local:

------
Python
------

Server Connection
=================

Ping the server::

    >>> import intelanalytics as ia
    >>> ia.server.ping()
    Successful ping to Intel Analytics at http://localhost:9099/info
    >>> ia.connect()

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

By default the toolkit does not print the full stack trace when exceptions occur.  To see the full Python stack trace of the last (i.e. most recent) exception::

    ia.errors.last

To enable always printing the full Python stack trace, set the 'show_details' property::

    import intelanalytics as ia
     
    # show full stack traces
    ia.errors.show_details = True
     
    ia.connect()
     
    # … the rest of your script …

If you enable this setting at the top of your script you get better error
messages.
The longer error messages are really helpful in bug reports, emails about
issues, etc.
 
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

Resolving disk full issue while running Spark jobs
==================================================

If you are using a Red Hat cluster or an old CentOS cluster, due to the way the /tmp file system is setup, 
you might see that while running spark jobs, your /tmp drive becomes full and causes the jobs to fail.

This is because Spark and other :abbr:`CDH (Cloudera Hadoop)` services, by default use /tmp as the temporary location to store files required during 
run time including but not limited to shuffle data.

In order to resolve this, follow these instructions:

1)  Stop the Intelanalytics service

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
Whenever you run a Spark application, jars and logs go to /va/run/spark/work (or other location if configured in Cloudera Manager).
These can use up a bit of space eventually (over 140MB per command).

* Short-term workaround: periodically delete these files
* Long-term fix: Spark 1.0 will automatically clean up the files

----------
References
----------

`Spark Docs <https://spark.apache.org/documentation.html>`__


