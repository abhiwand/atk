===========================
PyCharm Setup Instructions
===========================

PyCharm is a Python IDE created by JetBrains.

.. contents:: Table of Contents
    :local:

-------------
Prerequisites
-------------

These instructions assume you have already installed

- Python 2.6 
- Intel Analytics Python Rest Client and required dependencies
- `PyCharm <http://www.jetbrains.com/pycharm/>`_

The Intel Analytics Toolkit should work with any version of PyCharm but these instructions were tested with PyCharm
Community Edition 3.4.1.

-----
Setup
-----

1)  Select "New Project" on PyCharm's initial screen

    #)  Give your project a name, e.g "myproject"

    #)  Choose "Python 2.6" as the Python Interpreter and choose the "OK" button


        i)  If "Python 2.6" does not appear in the list you will need to configure a Python 2.6 Intepreter.

            1)  Choose the button that looks like a "gear"

            #)  Choose "Add Local"

            #)  Browse for your local Python 2.6 installation.  On RedHat or Centos this is probably /usr/bin/python.

            #)  Choose the "OK" button


#)  Choose :menuselection:`File --> Settings`

    a)  Choose "Project Structure"

    #)  Choose "Add Content Root" and browse to the Intel Analytics Python Rest Client libraries.  On RedHat or Centos
        these are found under "/usr/lib/intelanalytics/rest-client/python".

    #)  Choose "Apply" button

    #)  Choose "OK" button


#)  Choose :menuselection:`File --> New --> Python File`


#)  Name the file "test" and type in the following code::

        import intelanalytics as ia
        ia.server.ping()


#)  Choose :menuselection:`Run --> Run`, you should see the output::

        Successful ping to Intel Analytics at http://localhost:9099/info

#)  Next take a look at the included examples

