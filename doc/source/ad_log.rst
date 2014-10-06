=======================
Intel Analytics Logging
=======================

.. contents:: Table of Contents
    :local:

------------
Introduction
------------

Logging, in Intel Analytics service, is done with the help of LOGback.
The full documentation for LOGback can be found on their site.
For a deep dive into LOGback please refer to their documentation, as we will only cover the basics.

---------
Log Files
---------

The Intel Analytics service writes two log files to the system, both of which are located in ``/var/log/intelanalytics/rest-server/``.

output.log
==========

Contains all log messages sent to the console.
This will contain messages from many of the services Intel Analytics uses like spark, yarn, hdfs, etc. as well as the Intel Analytics service.

application.log
===============

Contains log messages from the Intel Analytics service only.

----------
Log Levels
----------

The possible log levels for Intel Analytics are the same as those that are available for LOGback.

*   TRACE
*   DEBUG
*   INFO
*   WARN
*   ERROR

Updating The Log Level
======================

Changing the log level for the Intel Analytics service is easy.

Open The Configuration File
---------------------------
First, open the configuration file::

    sudo vim /etc/intelanalytics/rest-server/logback.xml

You should see somethings like this::

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

After updating the level attribute, save the file and either restart the Intel Analytics service or wait one minute for the configuration to be reloaded.

.. warning::

    Be careful while changing the LOGback configuration.
    You might cause undue strain on the server or the Intel Analytics service by setting DEBUG logging level in a production environment.

