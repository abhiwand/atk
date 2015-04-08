Configuration Script
--------------------
.. only:: html

    Sample output with notes::

        #if the default is correct hit enter
        $ sudo ./config

        What port is Cloudera Manager listening on? defaults to "7180" if nothing is entered:
        What is the Cloudera Manager username? defaults to "admin" if nothing is entered:
        What is the Cloudera Manager password? defaults to "admin" if nothing is entered:

        No current SPARK_CLASSPATH set.
        Setting to:
        SPARK_CLASSPATH="/usr/lib/intelanalytics/graphbuilder/lib/ispark-deps.jar"

        Deploying config   .   .   .   .   .   .   .   .   .   .   .   .
        Config Deployed

        You need to restart Spark service for the config changes to take affect.
        Would you like to restart now? Enter 'yes' to restart. defaults to 'no' if nothing is
        entered: yes
        Restarting Spark  .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .
        Restarted Spark


        What is the hostname of the database server? defaults to "localhost" if nothing is entered:
        What is the port of the database server? defaults to "5432" if nothing is entered:
        What is the name of the database? defaults to "ia_metastore" if nothing is entered:
        What is the database user name? defaults to "iauser" if nothing is entered:

        #The dollar sign($) is not allowed in the password.
        What is the database password? The default password was randomly generated.
            Defaults to "****************************" if nothing is entered:

        Creating application.conf file from application.conf.tpl
        Reading application.conf.tpl
        Updating configuration
        Configuration created for Intel Analytics
        Configuring postgres access for  "iauser"
        Initializing database:                                     [  OK  ]
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        CREATE ROLE
        0
        CREATE DATABASE
        0
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        initctl: Unknown instance:
        intelanalytics-rest-server start/running, process 17484
        0
        Waiting for Intel Analytics server to restart
         .   .   .   .
        You are now connected to database "ia_metastore".
        INSERT 0 1
        0
        postgres is configured
        Intel Analytics is ready for use.

.. only:: latex

    Sample output with notes::

        #if the default is correct hit enter
        $ sudo ./config

        What port is Cloudera Manager listening on?
            defaults to "7180" if nothing is entered:
        What is the Cloudera Manager username?
            defaults to "admin" if nothing is entered:
        What is the Cloudera Manager password?
            defaults to "admin" if nothing is entered:

        No current SPARK_CLASSPATH set.
        Setting to:
        SPARK_CLASSPATH="/usr/lib/intelanalytics/graphbuilder/lib/ispark-deps.jar"

        Deploying config   .   .   .   .   .   .   .   .   .   .   .   .
        Config Deployed

        You need to restart Spark service for the config changes to take affect.
        Would you like to restart now? Enter 'yes' to restart. defaults to 'no' if nothing is
        entered: yes
        Restarting Spark  .   .   .   .   .   .   .   .   .   .   .   .   .   .   .
        Restarted Spark


        What is the hostname of the database server?
            defaults to "localhost" if nothing is entered:
        What is the port of the database server?
            defaults to "5432" if nothing is entered:
        What is the name of the database?
            defaults to "ia_metastore" if nothing is entered:
        What is the database user name?
            defaults to "iauser" if nothing is entered:

        #The dollar sign($) is not allowed in the password.
        What is the database password?
            The default password was randomly generated.
            Defaults to "****************************" if nothing is entered.

        Creating application.conf file from application.conf.tpl
        Reading application.conf.tpl
        Updating configuration
        Configuration created for Intel Analytics
        Configuring postgres access for  "iauser"
        Initializing database:                                     [  OK  ]
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        CREATE ROLE
        0
        CREATE DATABASE
        0
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        initctl: Unknown instance:
        intelanalytics-rest-server start/running, process 17484
        0
        Waiting for Intel Analytics server to restart
         .   .   .   .
        You are now connected to database "ia_metastore".
        INSERT 0 1
        0
        postgres is configured
        Intel Analytics is ready for use.


