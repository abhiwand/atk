===========================
IntelliJ Setup Instructions
===========================

.. contents:: Table of Contents
    :local:

-------------
Prerequisites
-------------

These instructions assume you have already installed

- Python 2.6 
- Intel Analytics Python Rest Client and required dependencies
- `IntelliJ IDEA <http://www.jetbrains.com/idea/>`_

Verify the "Python" plugin is enabled in IntelliJ by choosing :menuselection:`File --> Settings`,
searching for "Python", and choosing "Plugins" from the pane on the left-hand side.

The Intel Analytics Toolkit should work with any version of IntelliJ IDEA but these instructions were tested
with IntelliJ IDEA 13.1.3 Ultimate.

-----
Setup
-----
1)  Select "New Project" on IntelliJ IDEA's initial screen

#)  Select "Python" as the project type and choose "Next"


    a)  Choose "Next" leaving "Create project form template" unchecked

    #)  Choose "Python 2.6" as the Python Interpreter and choose the "Next" button


        i)  If "Python 2.6" does not appear in the list you will need to configure a Python 2.6 Intepreter.

            1)  Choose the "Configure" button

            #)  Choose the plus sign "+"

            #)  Choose "Python SDK"

            #)  Choose "Add Local"

            #)  Browse for your local Python 2.6 installation.  On RedHat this is probably /usr/bin/python.

            #)  Choose the "OK" button

    #)  Give your project a name, e.g "myproject"

    #)  Choose the "Finish" button


#)  Choose :menuselection:`File --> Project Structure`

    a)  Make sure "Python 2.6" is selected as the Project SDK and choose "Apply"

    #)  Choose "Libraries" in the left hand pane

    #)  Choose the plus sign "+"

    #)  Choose "Java" and browse to the Intel Analytics Python Rest Client libraries.  On RedHat these are found under "/usr/lib/intelanalytics/rest-client/python".

    #)  Choose "classes"

    #)  Choose "ia" and click "OK" button

    #)  Name the library "ia-python-client"

    #)  Choose "OK" button


#)  Choose :menuselection:`File --> New --> Python File`


#)  Name the file "test" and type in the following code::

        import intelanalytics as ia
        ia.server.ping()


#)  Choose :menuselection:`Run --> Run`, you should see the output::

        Successful ping to Intel Analytics at http://localhost:9099/info

#)  Next take a look at the included examples

