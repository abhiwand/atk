-------------------------
|IA| Packages Information
-------------------------

The dependency list is merely informational.
When yum installs a package, it will pull dependencies automatically.
All the Cloudera dependencies are implied for all packages.

|IA| REST Server
================
Only needs to be installed on a single node.

Package Name: intelanalytics-rest-server

Dependencies

i.  intelanalytics-python-client
#.  intelanalytics-graphbuilder
#.  python-cm-api
#.  python-argparse
#.  Java Runtime Environment or Java Development Environment 1.7

|IA| Python Client
==================
Needs to be installed on every spark worker node as well as the gateway node
or other node that is going to be the designated client.
The IA python client submitting requests, the rest server and the rest client
package installed on the worker nodes must all be the same version.

Package Name: intelanalytics-python-rest-client

Dependencies

i.  python 2.7
#.  `python27-ordreddict <https://pypi.python.org/pypi/ordereddict>`_
#.  `python27-numpy <https://pypi.python.org/pypi/numpy>`_ >= 1.81
#.  `python27-bottle <https://pypi.python.org/pypi/bottle>`_ >= 0.12
#.  `python27-requests <https://pypi.python.org/pypi/requests>`_ >= 2.2.1


|IA| Graph Builder
==================
Needs to be installed with the IA rest server

Package Name: intelanalytics-graphbuilder

Dependencies

*   intelanalytics-spark-deps

|IA| Spark Dependencies
=======================
Needs to be installed on every individual spark worker node.

Package Name: intelanalytics-spark-deps

Dependencies

*   none

