==========
PostgreSQL
==========

The Intel Analytics application uses H2, an in-memory database that is lost on application restart.
This is convenient for testing.
H2 setup is completely automatic.
No steps below are needed for H2.

By default, the Cloudera Manager installs PostgreSQL_ which is used for tracking metadata.
PostgreSQL is only required if your engine and API server are on different nodes or if you are in a more production like environment (where you want your data to persist between restarts).

--------------------------------------
Verify PostgreSQL is already installed
--------------------------------------
+---------------------------------------+---------------------------------------+
| RedHat/CentOS                         | Ubuntu                                |
+=======================================+=======================================+
| ::                                    | ::                                    |
|                                       |                                       |
|     yum info postgresql               |     TBD                               |
+---------------------------------------+---------------------------------------+

--------------------
Configure PostgreSQL
--------------------

Initialize the database
=======================
Switch user to the PostgreSQL user::

    sudo su postgres

Change to the data folder::

    cd /var/lib/pgsql/data

If the data folder is empty, you need to initialize it.

+---------------------------------------+---------------------------------------+
| RedHat/CentOS                         | Ubuntu                                |
+=======================================+=======================================+
| ::                                    | ::                                    |
|                                       |                                       |
|     sudo service postgresql initdb    |     TBD                               |
+---------------------------------------+---------------------------------------+


Tell PostgreSQL where to listen
===============================
Modify 'pg_hba.conf' adding a line with the IP that PostgreSQL will listen on for connections, add this before the other lines in the file.

If the Engine and the PostgreSQL are on the same node::

    # TYPE  DATABASE    USER        CIDR-ADDRESS          METHOD
    host    all         metastore   127.0.0.1/32          md5

If Engine and PostgreSQL are on different nodes::

    # TYPE    DATABASE    USER        CIDR-ADDRESS               METHOD
    host      all         metastore   <hostname of server>/32    md5

Modify 'postgresql.conf' and uncomment the *listen_addresses* setting with IP's that PostgreSQL should listen on.

If the engine and PostgreSQL are on the same node:
    Lock PostgreSQL to listen on the local loopback interface only: listen_addresses = 'localhost'
If the engine and PostgreSQL are on different nodes:
    Allow PostgreSQL to listen to both an externally accessible interface and the local loopback: listen_addresses = 'localhost,<hostname>'
    or to listen on all interfaces: listen_addresses = '*'

Restart PostgreSQL::

    sudo service postgresql restart

Create a metastore user and database
====================================
Run::

    psql

Create user ``metastore`` with createdb encrypted password "Tribeca123".

Create database ``metastore`` with owner "metastore".
It is also good to create user for yourself so you don't have to sudo all of the time.
Create user <yourUserName> with superuser powers.

Configure the application to use PostgreSQL
===========================================
Edit 'source_code/api-server/src/main/resources/application.conf'

Comment out H2 configuration

Uncomment PostgreSQL configuration

Restart the api-server.
The api-server will automatically initialize the database at start-up time.

.. ifconfig:: internal_docs

    Start our application, it will create the schema automatically using Flyway
    Use \d to see the schema, see the cheatsheet

Insert a user
=============
::

    psql metastore
    insert into users (username, api_key, created_on, modified_on) values( 'my-test-user', 'test_api_key_1', now(), now() );

See also:

..  toctree::
    :maxdepth: 1
    
    ad_psql_cs

.. _PostgreSQL: http://www.postgresql.org
