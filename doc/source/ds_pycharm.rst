.. index:: ! PyCharm
    single: IDE

==========================
PyCharm Setup Instructions
==========================

PyCharm is a Python IDE created by JetBrains.

.. contents:: Table of Contents
    :local:
    :backlinks: none

-------------
Prerequisites
-------------

These instructions assume you have already installed:

-   Python 2.7.
-   |IAT| Python REST Client and required dependencies.
-   `PyCharm <http://www.jetbrains.com/pycharm/>`_.

The |IAT| should work with any version of PyCharm but these instructions were
tested with PyCharm Community Edition 3.4.1.

-----
Setup
-----

1)  Select *New Project* on PyCharm's initial screen.

    #)  Give your project a name, for example, "myproject".
    #)  Choose *Python 2.7* as the Python Interpreter and choose the *OK*
        button.

        i)  If *Python 2.7* does not appear in the list you will need to
            configure a Python 2.7 Intepreter.

            1)  Choose the button that looks like a "gear".
            #)  Choose *Add Local*.
            #)  Browse for your local Python 2.7 installation.
                On RedHat or Centos this is probably /usr/bin/python.
            #)  Choose the *OK* button.

#)  Choose :menuselection:`File --> Settings`.

    a)  Choose *Project Structure*.
    #)  Choose *Add Content Root* and browse to the |IAT| Python REST Client
        libraries.
        On RedHat or Centos these are found under
        '/usr/lib/intelanalytics/rest-client/python'.
    #)  Choose *Apply* button.
    #)  Choose *OK* button.

#)  Right click your project folder, for example, "myproject", and select
    :menuselection:`New --> Python File`.

    a)  Name the file "test" and type in the following code::

            import intelanalytics as ia
            ia.server.host = "correct host name or IP address"
            ia.connect()
            ia.server.ping()

    #)  If you see a yellow bar across the top of the file warning about
        "Package requirements" not being satisfied then your system is not
        setup correctly.

        i)  You may not have installed all of the Python dependencies for the
            |IAT| REST Client correctly.
        #)  You may have chosen the wrong Python interpreter.


#)  Choose :menuselection:`Run --> Run`, you should see the output::

        Successful ping to Intel Analytics at http://localhost:9099/info

#)  Next take a look at the included examples.

