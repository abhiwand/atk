.. index:: ! log

=============
|IAT| Logging
=============

.. contents:: Table of Contents
    :local:

------------
Introduction
------------

Logging, in |IAT| service, is done with the help of LOGback.
Full documentation for LOGback can be found at http://logback.qos.ch/.

---------
Log Files
---------

The |IAT| service writes two log files to the system, both of which are located
in 'var/log/intelanalytics/rest-server/'.

output.log
==========

Contains all log messages sent to the console.
This will contain messages from many of the services |IAT| uses, like spark,
yarn, and hdfs, as well as the |IAT| service.

application.log
===============

Contains log messages from the |IAT| service only.

----------
Log Levels
----------

The possible log levels for |IAT| are the same as those that are available for
LOGback.

*   TRACE
*   DEBUG
*   INFO
*   WARN
*   ERROR

Updating The Log Level
======================

Changing the log level for the |IAT| service is easy.

Open The Configuration File
---------------------------
First, open the configuration file::

    $ sudo vim /etc/intelanalytics/rest-server/logback.xml

The file should be something like this::

    <configuration scan="true">
        <appender name="FILE" class="ch.qos.logback.core.FileAppender">
            <file>/var/log/intelanalytics/rest-server/application.log</file>
            <encoder>
                <pattern>%date - [%level] - from %logger in %thread %message
                    %n%ex{full}%n</pattern>
            </encoder>
        </appender>
        <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
            <encoder>
                <pattern>%logger{15} - %message %n %ex{short}%n</pattern>
            </encoder>
        </appender>
        <logger name="play" level="INFO" />
        <logger name="application" level="INFO" />
        <!--
            log levels
            TRACE
            DEBUG
            INFO
            WARN
            ERROR
        -->
    #update the level attribute to any of the above log levels::
        <root level="INFO">
            <appender-ref ref="FILE" />
            <appender-ref ref="STDOUT" />
        </root>
    </configuration>

Update The Logging Level
------------------------

Update the "level" attribute for the "root" xml tag::

    ...
    #update the level attribute to any valid logging level
        <root level="UPDATE ME">
            <appender-ref ref="FILE" />
            <appender-ref ref="STDOUT" />
        </root>
    ...

After updating the level attribute, save the file and either restart the |IAT|
service or wait one minute for the configuration to be reloaded.

.. warning::

    Be careful while changing the LOGback configuration.
    It is possible to cause undue strain on the server or the |IAT| service by
    setting the DEBUG logging level in a production environment.

