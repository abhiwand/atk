Installation
============

The installation of the Intel® Analytics package relys upon other programs to function properly. During installation these other packages are checked for and installed if absent:
    python2.7 python2.7-pip python2.7-pandas
    'bottle' version 0.12 or newer
    'cloud' version 2.8.5 or newer
    'numpy' version 1.8.1 or newer
    'setuptools' version 3.6 or newer
    'requests' version 2.2.1 or newer
In addition, the rest-server package also installs 'java' version 1.7 or newer

.. toctree::
   :maxdepth: 1

   ad_yum

.. ad_apt (future)

Effects
-------
Files installed as part of Intelanalytics-rest-server package:
    /etc/init.d/intelanalytics-rest-server
    /etc/init/intelanalytics-rest-server.conf
    /etc/intelanalytics/rest-server/application.conf
    /etc/intelanalytics/rest-server/logback.xml
    /usr/lib/intelanalytics/rest-server/launcher.jar
    /usr/lib/intelanalytics/rest-server/lib/api-server.jar
    /usr/lib/intelanalytics/rest-server/lib/engine-spark.jar
    /usr/lib/intelanalytics/rest-server/lib/engine.jar
    /usr/lib/intelanalytics/rest-server/lib/interfaces.jar

Files installed as part of Intelanalytics-rest-client package:
    /usr/lib/intelanalytics/rest-client/python/__init__.py
    /usr/lib/intelanalytics/rest-client/python/core/__init__.py
    /usr/lib/intelanalytics/rest-client/python/core/backend.py
    /usr/lib/intelanalytics/rest-client/python/core/column.py
    /usr/lib/intelanalytics/rest-client/python/core/config.py
    /usr/lib/intelanalytics/rest-client/python/core/files.py
    /usr/lib/intelanalytics/rest-client/python/core/frame.py
    /usr/lib/intelanalytics/rest-client/python/core/graph.py
    /usr/lib/intelanalytics/rest-client/python/core/loggers.py
    /usr/lib/intelanalytics/rest-client/python/core/row.py
    /usr/lib/intelanalytics/rest-client/python/core/serialize.py
    /usr/lib/intelanalytics/rest-client/python/core/sources.py
    /usr/lib/intelanalytics/rest-client/python/core/types.py
    /usr/lib/intelanalytics/rest-client/python/doc/Makefile
    /usr/lib/intelanalytics/rest-client/python/doc/__init__.py
    /usr/lib/intelanalytics/rest-client/python/doc/source/_static/strike.css
    /usr/lib/intelanalytics/rest-client/python/doc/source/conf.py
    /usr/lib/intelanalytics/rest-client/python/doc/source/examples.rst
    /usr/lib/intelanalytics/rest-client/python/doc/source/index.rst
    /usr/lib/intelanalytics/rest-client/python/doc/source/overapi.rst
    /usr/lib/intelanalytics/rest-client/python/doc/source/rowfunc.rst
    /usr/lib/intelanalytics/rest-client/python/little/__init__.py
    /usr/lib/intelanalytics/rest-client/python/little/frame.py
    /usr/lib/intelanalytics/rest-client/python/requirements.txt
    /usr/lib/intelanalytics/rest-client/python/rest/__init__.py
    /usr/lib/intelanalytics/rest-client/python/rest/connection.py
    /usr/lib/intelanalytics/rest-client/python/rest/frame.py
    /usr/lib/intelanalytics/rest-client/python/rest/graph.py
    /usr/lib/intelanalytics/rest-client/python/rest/hooks.py
    /usr/lib/intelanalytics/rest-client/python/rest/launcher.py
    /usr/lib/intelanalytics/rest-client/python/rest/message.py
    /usr/lib/intelanalytics/rest-client/python/rest/prettytable.py
    /usr/lib/intelanalytics/rest-client/python/rest/serialize.py
    /usr/lib/intelanalytics/rest-client/python/rest/spark.py
    /usr/lib/intelanalytics/rest-client/python/tests/.gitignore
    /usr/lib/intelanalytics/rest-client/python/tests/__init__.py
    /usr/lib/intelanalytics/rest-client/python/tests/exec_all.sh
    /usr/lib/intelanalytics/rest-client/python/tests/iatest.py
    /usr/lib/intelanalytics/rest-client/python/tests/run_doctests.sh
    /usr/lib/intelanalytics/rest-client/python/tests/test_core_files.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_core_frame.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_core_types.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_little_frame.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_rest_api.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_rest_connection.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_rest_frame.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_rest_row.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_serialize.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_sources.py
    /usr/lib/intelanalytics/rest-client/python/tests/test_webhook.py
    symbolic link from /usr/lib/intelanalytics/rest-client/python  to /usr/lib/python2.7/site-packages/intelanalytics

