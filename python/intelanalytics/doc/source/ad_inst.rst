Installation
============

Pre-Installation
----------------

Set Up A Cluster Server
~~~~~~~~~~~~~~~~~~~~~~~

Server Workstation
``````````````````

A user needs to be set up on the workstation with superuser rights without a password, for example ``hadoop``.
    $ sudo visudo
Add the following line to the end of file
    hadoop ALL=(ALL)      NOPASSWD: ALL
Use a fully qualified hostname wherever it can be used, for example ``host.computercenter.com``.
To assign a hostname to the workstation in Ubuntu it is necessary to edit the file /etc/hostname; in RedHat/CentOS it is /etc/sysconfig/network.
    Add or change the hostname to ``host.computercenter.com``
    Save the change
    Run the command $ sudo hostname ``host.computercenter.com``
The next step is to set up the hosts file.
    Edit the file /etc/hosts
    Add the actual ip address and then the hostname, for example: ``127.0.0.1`` ``host.computercenter.com``
    The actual hostname should be just after the ip address in a single line.
Now, turn off iptables and selinux.
In Ubuntu, the steps to turn off the iptables are:
    sudo service ufw stop
    sudo ufw disable
While in RedHat/CentOS, the steps are:
    sudo service iptables stop
    sudo chkconfig iptables off
To turn off selinux, edit the file /etc/sysconfig/selinux and change the SELINUX line to:
    SELINUX=disabled
Keep RedHat from defragmenting the hard drive.
    Edit /etc/rc.local
    At the bottom of the file enter: echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag

A cluster server is usually a dedicated group of computers, but for testing and trial uses, it could be the same computer as the client.
Install the Cloudera Manager. See `Cloudera Manager`_ and `Cloudera Documentation`_.
This sets up a data server.

PostgreSQL
~~~~~~~~~~

Under normal circumstances, when you installed the Cloudera Manager, it will have also installed PostgreSQL.
This program is used for managing the metadata (the user login and the command record)

Installation
------------

| The installation of the Intel® Analytics package relys upon other programs to function properly. During installation these other packages are checked for and installed if absent:
|     python2.7
|     python2.7-pip
|     python2.7-pandas
|     'bottle' version 0.12 or newer
|     'cloud' version 2.8.5 or newer
|     'numpy' version 1.8.1 or newer
|     'setuptools' version 3.6 or newer
|     'requests' version 2.2.1 or newer

In addition, the rest-server package also installs 'java' version 1.7 or newer

Platform Specific Installation
------------------------------

.. toctree::
   :maxdepth: 1

   ad_yum

.. ad_apt (future)

sudo visudo
Defaults:rmaldrix !requiretty
rmaldrix        ALL=(ALL)       NOPASSWD: ALL
127.0.0.1 because 10.10.68.36 did not work (personal problem)




Effects
-------
| Files installed as part of Intelanalytics-rest-server package:
|     /etc/init.d/intelanalytics-rest-server
|     /etc/init/intelanalytics-rest-server.conf
|     /etc/intelanalytics/rest-server/application.conf
|     /etc/intelanalytics/rest-server/logback.xml
|     /usr/lib/intelanalytics/rest-server/launcher.jar
|     /usr/lib/intelanalytics/rest-server/lib/api-server.jar
|     /usr/lib/intelanalytics/rest-server/lib/engine-spark.jar
|     /usr/lib/intelanalytics/rest-server/lib/engine.jar
|     /usr/lib/intelanalytics/rest-server/lib/interfaces.jar
| 
| Files installed as part of Intelanalytics-rest-client package:
|     /usr/lib/intelanalytics/rest-client/python/__init__.py
|     /usr/lib/intelanalytics/rest-client/python/core/__init__.py
|     /usr/lib/intelanalytics/rest-client/python/core/backend.py
|     /usr/lib/intelanalytics/rest-client/python/core/column.py
|     /usr/lib/intelanalytics/rest-client/python/core/config.py
|     /usr/lib/intelanalytics/rest-client/python/core/files.py
|     /usr/lib/intelanalytics/rest-client/python/core/frame.py
|     /usr/lib/intelanalytics/rest-client/python/core/graph.py
|     /usr/lib/intelanalytics/rest-client/python/core/loggers.py
|     /usr/lib/intelanalytics/rest-client/python/core/row.py
|     /usr/lib/intelanalytics/rest-client/python/core/serialize.py
|     /usr/lib/intelanalytics/rest-client/python/core/sources.py
|     /usr/lib/intelanalytics/rest-client/python/core/types.py
|     /usr/lib/intelanalytics/rest-client/python/doc/Makefile
|     /usr/lib/intelanalytics/rest-client/python/doc/__init__.py
|     /usr/lib/intelanalytics/rest-client/python/doc/source/_static/strike.css
|     /usr/lib/intelanalytics/rest-client/python/doc/source/conf.py
|     /usr/lib/intelanalytics/rest-client/python/doc/source/examples.rst
|     /usr/lib/intelanalytics/rest-client/python/doc/source/index.rst
|     /usr/lib/intelanalytics/rest-client/python/doc/source/overapi.rst
|     /usr/lib/intelanalytics/rest-client/python/doc/source/rowfunc.rst
|     /usr/lib/intelanalytics/rest-client/python/little/__init__.py
|     /usr/lib/intelanalytics/rest-client/python/little/frame.py
|     /usr/lib/intelanalytics/rest-client/python/requirements.txt
|     /usr/lib/intelanalytics/rest-client/python/rest/__init__.py
|     /usr/lib/intelanalytics/rest-client/python/rest/connection.py
|     /usr/lib/intelanalytics/rest-client/python/rest/frame.py
|     /usr/lib/intelanalytics/rest-client/python/rest/graph.py
|     /usr/lib/intelanalytics/rest-client/python/rest/hooks.py
|     /usr/lib/intelanalytics/rest-client/python/rest/launcher.py
|     /usr/lib/intelanalytics/rest-client/python/rest/message.py
|     /usr/lib/intelanalytics/rest-client/python/rest/prettytable.py
|     /usr/lib/intelanalytics/rest-client/python/rest/serialize.py
|     /usr/lib/intelanalytics/rest-client/python/rest/spark.py
|     /usr/lib/intelanalytics/rest-client/python/tests/.gitignore
|     /usr/lib/intelanalytics/rest-client/python/tests/__init__.py
|     /usr/lib/intelanalytics/rest-client/python/tests/exec_all.sh
|     /usr/lib/intelanalytics/rest-client/python/tests/iatest.py
|     /usr/lib/intelanalytics/rest-client/python/tests/run_doctests.sh
|     /usr/lib/intelanalytics/rest-client/python/tests/test_core_files.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_core_frame.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_core_types.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_little_frame.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_rest_api.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_rest_connection.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_rest_frame.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_rest_row.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_serialize.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_sources.py
|     /usr/lib/intelanalytics/rest-client/python/tests/test_webhook.py
|     symbolic link from /usr/lib/intelanalytics/rest-client/python  to /usr/lib/python2.7/site-packages/intelanalytics

.. _Cloudera Manager: http://www.cloudera.com/content/support/en/downloads/cloudera_manager/cm-5-0-2.html
.. _Cloudera Documentation: http://www.cloudera.com/content/support/en/documentation/cdh5-documentation/cdh5-documentation-v5-latest.html
