============================
Building and Running Tribeca
============================

To build ititan dependency::

    cd ititan
    mvn clean install

To build the rest of the project::

    mvn clean install


That will build a bunch of jar files.
Once you’ve built them once, you can just use compile instead of assembly -- it’s faster,
and the Tribeca launcher prioritizes your loose class files over the class files in the jar,
so we only need to build the jar once to pull in any dependencies.
It can be handy to put sbt into “watcher mode” so it watches for changes to any of the source files and automatically recompiles:

> \~compile

The ~ works with any command, not just \~compile.
Once we have the unit tests in and working, \~test might be an even better one to use.

You’re going to launch the system using “laucher.jar” -- a command for your system would look like
(assuming you want to store your dataframes in \~/intelanalytics)::

    bin/api-server.sh

This will launch the api server.
Right now the api server has an embedded version of the engine in it, so this is the process that will be running your code.
Currently it’s configured to use spark in local mode for easier testing at this point,
which is why I mentioned \~/intelanalytics above -- it will only write to your local filesystem
(it’s using the Hadoop and Spark APIs, so this will be easy to change when we’re ready for it).

At this early point in the development, it is recommended to clear out the "hdfs" filesystem first::

    rm -rf ~/intelanalytics/*;bin/api-server.sh

Once this is done, you will have the API server running.
Point your browser (or REST tool like the `Postman REST client`_) at **\http://<your-server-name>:9099/v1/dataframes** as a starting point.

Note that you can override settings (which live in api-server/src/main/resources/application.conf) in one of two ways.
You can change a specific setting on the command line.
For instance, you might change the port that the api server will start on::

    rm \-rf \~/intelanalytics/; bin/api-server.sh \-Dintel.analytics.api.port=9999

You can also maintain a file of your preferred settings, and use that to override the settings.
Suppose you had a file called "dev.conf" in your current folder that contained this text::

    intel.analytics.api {
        host = "127.0.0.1"
        port = 3333
    }

If you include that file in the launcher's classpath, and tell the configuration system about it,
those values will override the defaults, and your server will start up only listening on 127.0.0.1 on port 3333.

*EXTRA_CONF=/path/to/folder/containing/my/dev.conf/file* rm \-rf \~/intelanalytics/; bin/api-server.sh *\-Dconfig.resource=dev.conf*

--------------------------
Installing ispark-deps.jar
--------------------------

When you run your Maven build, a jar gets created at source_code/ispark-deps/target/ispark-deps.jar.
This jar will get a new name every time it changes.
This means you don't have to re-install it every build, only when its version changes, for example 0.2, 0.3.
Copy this jar to all nodes in the cluster.
Make sure the spark user has access to the jar, including all parent directories.
(This is a common source of problems.)
Modify ``/etc/spark/conf/spark-env.sh`` add a line::

    export SPARK_CLASSPATH=/the/full/path/to/ispark-deps-0.1.jar

This needs to be done on EVERY node.
Or update the client configuration and deploy it from the CDH manager.
Restart Spark.

----------------
Using PostgreSQL
----------------

By default, our application uses H2, an in-memory database that is lost on application restart.
This is convenient for testing.
H2 setup is completely automatic.
No steps below are needed for H2.

By default, the Cloudera Manager installs PostgreSQL_ which is used for tracking metadata.
PostgreSQL is only required if your engine and API server are on different nodes or if you are in a more production like
environment (where you want your data to persist between restarts).

*   On your './interfaces/src/main/resources/reference.conf' or your 'application.conf' (if you are using RPM packages) set the following::

    *   metastore.connection-postgresql.host = "localhost"
    *   metastore.connection.url = "jdbc:postgresql://"${intel.analytics.metastore.connection-postgresql.host}
        ":"${intel.analytics.metastore.connection-postgresql.port}"/"${intel.analytics.metastore.connection-postgresql.database}

*   Configure PostgreSQL to use password authentication

    *   Verify postgresql is already installed::

            yum info postgresql

    *   If the data folder 'cd /var/lib/pgsql/data' is empty, you need to run::

            sudo service postgresql initdb

    *   Switch to the Postgres user::

            sudo su postgres
    
    *   cd to the data folder::

            cd /var/lib/pgsql/data
    
    *   Modify *pg_hba.conf*, adding a line with the IP that PostgreSQL will listen on for connections, add this **before** the other lines in the file.
    
        *   If Engine and PostgreSQL are on the same node::

                TYPE    DATABASE    USER        CIDR-ADDRESS    METHOD  
                host    all         metastore   127.0.0.1/32    md5
    
        *   If Engine and PostgreSQL are on different nodes::

                TYPE    DATABASE    USER        CIDR-ADDRESS                METHOD
                host    all         metastore   <IP of Engine Server>/32    md5
    
    *   Modify ``postgresql.conf`` and uncomment the *listen_addresses* setting with the IP that PostgreSQL should listen on.
    
        *   If the engine and API server are on the same host as PostgreSQL, you can just use "localhost", otherwise you need an IP or hostname.
        *   If Engine and PostgreSQL are on the same node lock PostgreSQL to listen on the local loopback interface only
        
            *   listen_addresses = 'localhost'
            
        *   If Engine and PostgreSQL are on different nodes allow PostgreSQL to listen to both an externally accessible interface and the local loopback

                listen_addresses = 'localhost,<IP of Accessible Interface>'
                
        *   or to listen on all interfaces
            
                listen_addresses = '*'
                
*   Restart PostgreSQL
::

        sudo service postgresql restart
    
*   Create a metastore user and database

    *   Run *psql*
    *   create user metastore with createdb with encrypted password 'Tribeca123'
    *   create database metastore with owner metastore
    *   It is also good to create a user for yourself so you don't have to ``sudo`` all of the time
    
        * create user yourUserName with superuser; // etc
        
*   Configure our application to use PostgreSQL

    *   Edit ``source_code/api-server/src/main/resources/application.conf``
    *   Comment out H2 configuration
    *   Uncomment PostgreSQL configuration
    
        *   If Engine and PostgreSQL are on different nodes replace the
        
*   Start our application, it will create the schema automatically using Flyway

    *   Use *\d* to see the schema, see the `cheatsheet <ad_psql_cs>`
    
*   Insert a user

        psql metastore
        insert into users (username, api_key, created_on, modified_on)
            values( 'metastore', 'test_api_key_1', now(), now() )

-------------
Related Pages
-------------

* :doc: ad_inst
* :doc: ad_psql_cs
* [GAO:Setting up Tribeca in CDH5]

.. ifconfig:: internal_docs

    * [GAO:IntelliJ Setup]
    * [GAO:Proxy Settings]



.. _PostgreSQL: http://www.postgresql.org
.. _`Postman REST client`: https://chrome.google.com/webstore/detail/postman-rest-client/fdmmgilgnpjigdojojpjoooidkmcomcm?hl=en