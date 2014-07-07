======================
Plugin Authoring Guide
======================
 
------------
Introduction
------------

The Intel Analytics Toolkit provides an extensibility framework that allows new commands and algorithms to be added to the system at runtime, without need of possessing the source code, nor recompiling the application.

Plug-ins should be easy to write, and should not require the author to have a deep understanding of the REST server, the execution engine, or any of its supporting libraries. In particular, plug-in authors should not have to understand how to manage multiple threads of execution, user authentication, marshaling of data to and from JSON, etc.

Plug-ins should also be isolated from the application as a whole, as well as from other plug-ins. Each plug-in should be allowed to use whatever libraries it needs, without concern for conflicts with the libraries that the Intel Analytics Toolkit uses for its own needs.

----------------
Types of Plugins
----------------

Commands and Queries
====================

The most common kinds of plugins are primarily divided into two categories, commands and queries. Commands are actions that are typically initiated by user, that have some impact on the system, such as loading a data frame, or removing columns from one.

Queries are also initiated by users, but their purpose is to return data to a client, with no side effects. The interfaces that command and query plug-ins implement are very similar, but it is important to use the correct interface so that the system can preserve the expected performance and semantics.

The outputs of commands and queries, and the processing of them, are monitored by the Intel Analytics processing engine.

----------------------
When to Write a Plugin
----------------------

Many of the operations and algorithms that you will want to express can be written in Python using the Python client. However, some kinds of operations are inconvenient to express in that format, or require better performance than Python can provide.

Anytime you have a new function such as an analytical or machine learning algorithm that you want to publish for use via the Python client or REST server, you should consider writing a CommandPlugin.

-----------------------
Plugin Support Services
-----------------------

Plugin Life Cycle
=================

Plugins will be loaded at application start up, and a start() method will be called to let them perform any one-time setup they need to do. When the application ends, a stop() method will be called to let the plugin clean up any state it had. Calling stop(), or allowing stop() to complete, is not guaranteed, depending on how the server is terminated.

Each invocation resulting from a user action or other source will provide an execution context object that encapsulates the arguments passed by the user as well as other relevant metadata.

Pagination
==========

Plugins that need to return large amounts of data to the user will be automatically paged in the same way that other Intel Analytics components handle pagination.

Logging and Error Handling
==========================

Errors that occur while running a plug-in will be trapped and reported in the same way that internal errors within the Intel Analytics Toolkit are normally trapped and reported.

Defaulting Arguments
====================

Authors should represent arguments that are not required using Option values. The system will supply default values for these optional values from the configuration system when the user’s invocation does not provide them.

Configuration for commands and queries should be included in the Typesafe Config configuration file associated with the application (defaults can be provided by a reference.conf in the plugin’s deployment jar). These begin at the “intel.analytics.component.command” and “intel.analytics.component.query” configuration point, and add additional keys based on the plugin’s location in the component tree. For example, the Loopy Belief Propagation plugin gets its configuration from “intel.analytics.component.command.graph.ml.loopy_belief_propagation”. Values that appear in this section are available to the plugin, and are passed to it during execution. Values in the .defaults subkey are used to provide the default argument values when the user does not specify them. The plugin does not have convenient access to other configuration parameters of the system, and plugin authors are strongly urged to take all configuration information from the Config instance they are passed rather than inspecting environment variables and so on.

Execution Flow
==============

.. image:: ad_plug_1.*

Accessing Spark or Other Components
===================================

For the time being, plugin authors may implement specific interfaces that declare their need for a particular service, e.g. SparkSupport for direct access to a SparkContext.

------------------------
Creating a CommandPlugin
------------------------

Naming
======

Naming your command correctly is crucial for the usability of your system. The Python client creates Python functions to match the commands in the engine, and it places them and names then in accordance with the name you specify for your plugin.

Name components are separated by slashes. For instance, the command that removes columns from a dataframe is called dataframe/remove_column. The Python client sees that name, knows that dataframe commands are associated with the BigFrame class, and therefore generates a function named remove_column on BigFrame. When the user calls that function, its arguments will be converted to JSON, sent to the REST server, and then on to the engine for processing. The results from the engine flow back through the REST server, and are converted back to Python objects.

If the name of the command contains more than one slash, the Python client will create intermediate objects that allow functions to be grouped logically together. For example, if the command is named dataframe/ml/my_new_algorithm (of course, your algorithms will have better names!), then the method created in the Python client could be accessed on a frame f using f.ml.my_new_algorithm(). You may nest commands as deeply as needed, any number of intermediary objects will be created automatically so that the object model of the frame or graph matches the command tree structure defined by the command names in the system.

REST Input and Output
=====================

Each command or query plug-in should define two case classes: one for arguments, and one for return value. The plug-in framework will ensure that the user’s Python (or JSON) commands are converted into an instance of the argument class, and the output from the plug-in will also be converted back to Python (or JSON) for storage in the command execution record for later return to the client.

Frame and Graph References
==========================

Usually, the commands associated with a frame or graph need to accept the frame or graph on which they should operate as a parameter. Use the class com.intel.intelanalytics.domain.frame.FrameReference to represent frames, and com.intel.intelanalytics.domain.graph.GraphReference to represent graphs.

Self Arguments
==============

Use a FrameReference as the type, and name the parameter “frame” or “dataframe” if you want this parameter to be filled by the BigFrame instance whose method is being invoked by the user. Similarly, if the method is on a graph, using  a GraphReference in a property named “graph” will do the trick for BigGraph instances.

-------------------
Creating an Archive
-------------------

Plugins are deployed in Archives – jar files that contain the plugin class, its argument and result classes, and any supporting classes it needs, along with a class that implements the Archive trait. The Archive trait provides the system with a directory of available services that the archive provides. On application start up, the application will query all the jar files it knows about (see below) to see what plugins they provide.

----------
Deployment
----------

Plug-Ins should be installed in the system using jar files. Jars that are found in the server’s lib directory will be available to be loaded based on configuration. The plug-ins that will be installed must be listed in the application .conf file. Each command or query advertises the location at which it would prefer to be installed in the URL structure, and if no further directives appear in configuration, they will be installed according to their request. However, using the configuration file, it is also possible to remap a plug-in to a different location or an additional location in the URL structure.

In the future, plugin discovery may be further automated, and it may also be possible to add a plugin without restarting the server.

-------------
Configuration
-------------

Server-side configuration should be stored in the reference.conf file for the plugin archive This is a Typesafe Config file (see https://github.com/typesafehub/config).


