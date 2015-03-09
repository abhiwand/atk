--------------------------
|IAT| Packages Information
--------------------------

The dependency list is merely informational.
When yum installs a package, it will pull dependencies automatically.
All the Cloudera dependencies are implied for all packages.

|IAT| Rest Server
=================
The rest server only needs to be installed on a single node.

Package Name: intelanalytics-rest-server

Dependencies

#.  intelanalytics-python-client
#.  intelanalytics-graphbuilder
#.  python-cm-api
#.  python-argparse
#.  Java Runtime Environment or Java Development Environment 1.7

|IAT| Python Client
===================
The Python client needs to be installed on every spark worker node as well as
the gateway node, or other nodes that are to be designated clients.
The |IAT| Python client submitting requests, the rest server and the rest
client package installed on the worker nodes must all be the same version.

Package Name: intelanalytics-python-rest-client

Dependencies

#.  Python 2.7
#.  `Python27-ordreddict <https://pypi.python.org/pypi/ordereddict>`_
#.  `Python27-numpy <https://pypi.python.org/pypi/numpy>`_ >= 1.81
#.  `Python27-bottle <https://pypi.python.org/pypi/bottle>`_ >= 0.12
#.  `Python27-requests <https://pypi.python.org/pypi/requests>`_ >= 2.2.1


|IA| Graph Builder
==================
Needs to be installed with the |IAT| rest server.

Package Name: intelanalytics-graphbuilder

Dependencies

*   intelanalytics-spark-deps

|IAT| Spark Dependencies
========================
Needs to be installed on every individual spark worker node.

Package Name: intelanalytics-spark-deps

Dependencies

*   none

