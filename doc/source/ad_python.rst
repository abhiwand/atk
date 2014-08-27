===================
Python Client Setup
===================

--------------------
Windows Python Setup
--------------------

1. Download Anaconda with python 2.7.

#. Install

   - By default, Anaconda installs to the user's AppData hidden folder. It's better to put this in a more accessible location, like ``c:\anaconda``. This is the only change from the default installation necessary.

#. Open a command prompt.

#. Run the command: ``conda create -n intelanalytics-python python=2.6 numpy=1.8 requests=2.3 ordereddict=1.1``. This creates a virtual python environment that mimics the cluster's configuration.

------------------------------------------
Install The Intel Analytics Toolkit Client
------------------------------------------

.. todo::
    - What is the right way to do this today?
    - What will we support in the future?

-------
Eclipse
-------

To configure Eclipse to use the right python environment:

1. Install PyDev.

   a. In Eclipse, navigate to :menuselection:`Help --> Eclipse Marketplace`.
   
   #. Search for "PyDev".
   
   #. Click Install.
   
   #. Accept the licence when prompted.

#. Configure python

   a. Navigate to :menuselection:`Windows --> Preferences --> PyDev --> Interpreters --> Python Interpreter`.
   
   #. Click "New...".
   
   #. Fill in the following values:
      
      - Name: intelanalytics-python
      
      - Interpreter Exe: 
        
        - Windows: ``c:\anaconda\envs\intelanalytics-python\python.exe``
      
          .. note::
          
             This assumes that you installed Anaconda to ``c:\anaconda``.
           
   #. Configure a python project to use intelanalytics-python
   
      i. In the "PyDev Package Explorer", right-click on the project and click "Properties" at the bottom of the menu.
      
      #. Click "PyDev - Interpreter/Grammar".
      
      #. Set the following values:
      
         - Choose a project type: Python
         
         - Grammar Version: 2.6
         
         - Interpreter: intelanalytics-python

#. See also :ref:`ad_bkm_ide`.