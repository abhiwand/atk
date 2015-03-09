==========================
IPython Setup Instructions
==========================

.. contents:: Table Of Contents
    :local:

------------
Introduction
------------

These instructions show how to use the |IAT| server through IPython.
This is a guide through IPython setup needed to communicate with an |IAT|
service on a remote cluster.
With an understanding of this information the Python rest client can be
accessed from a remote host through IPyhon shell or notebook server. 

------------
Requirements
------------

A working |IAT| cluster installation is required.
It must be configured to run with the Python2.7 executable.
If the |IAT| is not installed see :ref:`|IAT| Package Installation` to install
it.

Installing IPython
==================

To install IPython run::

    $ sudo yum install python27-ipython

---------------------------------
Configure |IA| Python Rest Client
---------------------------------

Before IPython can operate properly, it is necessary to configure the |IAT|
rest client.
The rest client needs to know where to find the |IA| rest server.
This is done by updating the host address in
/usr/lib/intelanalytics/rest-client/python/rest/config.py::

    $ sudo vim /usr/lib/intelanalytics/rest-client/python/rest/config.py

The config.py file will look similar to this::

    """
    config file for rest client
    """
    # default connection config
    class server:
        host = "localhost"
        port = 9099
        scheme = "http"
        version = "v1"
        headers = {'Content-type': 'application/json',
                'Accept': 'application/json,text/plain',
                'Authorization': "test_api_key_1"}

    class polling:
        start_interval_secs = 1
        max_interval_secs = 20
        backoff_factor = 1.02

    build_id = None

Update the address for host to the `Fully Qualified Domain Name
<http://en.wikipedia.org/wiki/Fully_qualified_domain_name>`_ or
the IP address of the node hosting the |IA| rest server.

---------------
Running IPython
---------------

Test the |IAT| IPython installation by importing the rest client libraries
inside of a notebook or IPython shell and ping the rest server.
::

    # testing IPython/Intel Analytics
    
    $ ipython
    Python 2.7.5 (default, Sep  4 2014, 17:06:50)
    Type "copyright", "credits" or "license" for more information.
    IPython 2.2.0 -- An enhanced Interactive Python.
    ?         -> Introduction and overview of IPython's features.
    %quickref -> Quick reference.
    help      -> Python's own help system.
    object?   -> Details about 'object', use 'object??' for extra details.

    In [1]: import intelanalytics as ia

    In [2]: ia.server.ping()
    Successful ping to Intel Analytics at http://localhost:9099/info

    In [3]: ia.connect()

IPython Notebook
================

All the dependencies to run the IPython notebook server are also installed
which lets the IPython shell be run from a web browser.
The notebook server is accessed by::
    
    $ ipython notebook

