=====================
PostgreSQL Cheatsheet
=====================

-------------------
Basic PSQL commands
-------------------

PSQL has a really nice command-line interface with full help and documentation build-in.

*psql dbname* \- launch the psql prompt and connect to a database

*\h* \- gives help on SQL, lists all of the possible commands

*\h create user* \- gives help on "create user"

*?* \- gives help on PSQL command line

*\d* \- lists all of the objects in your database

*\d tableName* \- describes a table

*\dt* \- lists all tables

*\q* \- quit

*\c* dbname - connect to another DB

.. ifconfig:: internal_docs

    ----------------------------------
    Drop and Recreate MetaStore Schema
    ----------------------------------

    psql metastore

    * \c postgres - connect to another DB like postgres
    * drop database metastore;
    * create database metastore;

    Restart the API server and Flyway_ (a library) will automatically recreate the schema.

    -------------------
    Flyway Introduction
    -------------------

    * Flyway is a Java library for automatically running SQL DDL migrations
    * Once a migration is merged into the sprint branch assume it has been ran in many locations
    * Never modify an old migration, only add new ones.
        * If you modify an existing migration, Flyway will fail because the checksum on the sql file is different.
    * See http://flywaydb.org/

    Flyway keeps version information in a table called "schema_version". &nbsp;You can select from this table to see what schema changes have been applied. ::

        metastore=# select * from schema_version;
         version_rank | installed_rank | version |       description       | type |             script              |  checksum   | installed_by |        installed_on        | execution_time | success
        -------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    1 |              1 | 1       | Initial version for 0.8l| SQL  | V1__Initial_version_for_0.8.sql | -1027169045 | metastore    | 2014-06-18 03:39:31.573302 |            363 | t
        (1 row)


.. _Flyway: http://flywaydb.org/
