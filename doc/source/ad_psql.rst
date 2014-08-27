============================================
Building and Running Intel Analytics Toolkit
============================================

----------------
Using PostgreSQL
----------------

By default, our application uses H2, an in-memory database that is lost on application restart.
This is convenient for testing.
H2 setup is completely automatic.
No steps below are needed for H2.

By default, the Cloudera Manager installs PostgreSQL_ which is used for tracking metadata.
PostgreSQL is only required if your engine and API server are on different nodes or if you are in a more production
environment (where you want your data to persist between restarts).

*   On your './interfaces/src/main/resources/reference.conf' or your 'application.conf' (if you are using RPM packages) set the following::

        metastore.connection-postgresql.host = "localhost"
        metastore.connection.url = "jdbc:postgresql://"${intel.analytics.metastore.connection-postgresql.host}":"${intel.analytics.metastore.connection-postgresql.port}"/"${intel.analytics.metastore.connection-postgresql.database}

*   Configure PostgreSQL to use password authentication

    *   Verify PostgreSQL is already installed::

            yum info postgresql
        
        If PostgreSQL is not already installed, you will need to install it before going on.

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
    
        *   If Engine and PostgreSQL are on the same node, lock PostgreSQL to listen on the local loopback interface only::
        
                listen_addresses = 'localhost'
            
        *   If Engine and PostgreSQL are on different nodes allow PostgreSQL to listen to both an externally accessible interface and the local loopback interface::

                listen_addresses = 'localhost,<IP of Accessible Interface>'
                
        *   or to listen on all interfaces::
            
                listen_addresses = '*'
                
*   Restart PostgreSQL

::

        sudo service postgresql restart
    
*   Create a metastore user and database

    *   Run *psql*
    *   create user metastore with createdb with encrypted password 'Tribeca123' (you can change it later)
    *   create database metastore with owner metastore
    *   It is also good to create a user for yourself so you don't have to ``sudo`` all of the time
    
        * create user yourUserName with superuser; // etc
        
*   Start our application, it will create the schema automatically using Flyway (which is installed by default).

    *   Use *\d* to see the schema, see the `cheatsheet <ad_psql_cs>`
    
*   Insert a user::

        psql metastore
        insert into users (username, API_key, created_on, modified_on)
            values( 'metastore', 'test_API_key_1', now(), now() )

-------------
Related Pages
-------------

:doc: ad_inst.rst

:doc: ad_psql_cs.rst

[GAO:Setting up Tribeca in CDH5]

.. ifconfig:: internal_docs

    * [GAO:IntelliJ Setup]
    * [GAO:Proxy Settings]

|

<- :doc:`ad_inst_IA`
<------------------------------->
:doc:`ad_psql_cs` ->

<- :doc:`ad_inst`

<- :doc:`index`

.. _PostgreSQL: http://www.postgresql.org
.. _`Postman REST client`: https://chrome.google.com/webstore/detail/postman-rest-client/fdmmgilgnpjigdojojpjoooidkmcomcm?hl=en