==========================
Eclipse Setup Instructions
==========================

.. contents:: Table of Contents
    :local:

-------------
Prerequisites
-------------

These instructions assume you have already installed

- Python 2.7 
- |IA| Python Rest Client and required dependencies
- Eclipse Standard

The |IA| Toolkit should work with any version of Eclipse but these instructions were tested with Eclipse Standard Version 4.4 Luna.

If you are on a corporate network, you may need to configure proxy settings in Eclipse before beginning (see Eclipse Help).

-----
Setup
-----
1)  Choose :menuselection:`Help --> Eclipse Marketplace`

#)  If the next screen does not come up you may need to configure proxy settings

#)  Search for "PyDev" and choose "PyDev - Python IDE for Eclipse 3.6.0" or newer version

    a)  Choose "Confirm" button"

    #)  Choose "Accept" when prompted for license agreement

    #)  If prompted, "Do you trust these certificates?" Choose "Brainwy Software; PyDev; Brainwy" and choose the "OK" button

    #)  When prompted to restart Eclipse choose the "Yes" button

#)  Choose the default Workspace

#)  Choose :menuselection:`File --> New --> Project...`

    a)  Choose the "PyDev" folder and "PyDev Project" and choose the "Next" button

    #)  Give your project a name, e.g. "myproject"

    #)  Choose version 2.7

    #)  Choose "Please configure an interpreter before proceeding"

        i)  Choose "Manual Configure"

        #)  Choose the "New" button

        #)  Browse for python 2.7.  On RedHat and Centos this is probably /usr/bin/python

        #)  Choose the "Ok" button

        #)  Choose the "Ok" button

    #)  Select the interpreter you just setup from the Interpreter drop-down

    #)  Choose the "Finish" button

    #)  When prompted "This kind of project is associated with the PyDev perspective. Do you want to open this perspective now?" choose "Yes"

#)  Right click your project folder, e.g. "myproject"

    a)  Choose "Properties"

    #)  Choose "PyDev - PYTHONPATH" in the left hand pane

    #)  Choose the "External Libraries" tab

    #)  Choose "Add source folder" button

    #)  Browse for the |IA| Python Rest Client libraries.  On RedHat and Centos these are found under "/usr/lib/intelanalytics/rest-client/python".

    #)  Choose the "OK" button

#)  Right click your project folder, e.g. "myproject"

    a)  Choose :menuselection:`New --> Source Folder`

    #)  Give it the name "src" and choose the "Finish" button

#)  Right click "src" folder and choose :menuselection:`New --> File`

    a)  Give the file name "test.py"

    #)  If prompted, confirm the default settings for PyDev by choosing "OK"

    #)  Close the "Help keeping PyDev alive" dialog, if it appears.

    #)  Type the following code into test.py::
    
            import intelanalytics as ia
            ia.server.host = "correct host name or IP address"
            ia.connect()
            ia.server.ping()

#)  Choose :menuselection:`Run --> Run`

    #)  Choose "Python Run" and choose the "OK" button, you should see the output::
    
            Successful ping to Intel Analytics at http://localhost:9099/info

#)  Next take a look at the included examples

.. |IA| replace:: Intel Analytics
