Intel Analytics Toolkit (ATK)
=============================

# Folders Overview
* bin/ - script for starting REST server, gremlin shell
* conf/ - configuration templates for setting up a system, put your application.conf here for running out of source_code
* deploy/ - a module for creating an uber jar needed for deployment on DP2.
* doc/ - end user docs for using the system (doc for specific plugins goes elsewhere)
* doc-api-examples/ - examples of api usage for end user docs
* engine/
  * engine-core/ - loads and executes plugins and provides the basic services that plugins need.
  * interfaces/ - interfaces the Engine exposes to the REST server. (we should move plugin args/return values out of here)
  * graphbuilder - Titan graph construction and reading
  * meta-store/ - code that interacts with the meta-store database repository including SQL scripts
* engine-plugins/ - plugins use engine services to implement user visible operations
  * frame-plugins/ - frame related plugins, e.g. frame.add_columns()
  * graph-plugins/ - graph related plugins that run on Spark and GraphX
  * model-plugins/ - model related plugins, e.g. LinearRegressionModel
  * giraph-plugins/ - a few algorithms that run on Giraph
* integration-tests/ - developer written, build time integration tests in python, these run against a minimal version of our product
* misc/ - miscellaneous items that aren't really part of our product
  * launcher/ - starts up our application, launches parts of our app
* package/ - packaging for VM's, RPM's
* python-client/ - python client code (talks with rest-server)
  * examples/ - example code for users of how to use the API
* rest-server/ - the rest server converts HTTP requests to Akka messages that are sent to the Engine
* testutils/ - some test utility code that gets reused between tests in different modules

# Building
* Install Python dependencies
* Run 'mvn install' for dependencies found under misc (event, event-api, titan-shaded)
* Run 'mvn install' at top level

# Running
* Build the project jars
* Create a conf/application.conf based on the templates under conf/examples
* Use bin/rest-server.sh to start the server
* cd /python
* ipython
  * import intelanalytics as ia
  * ia.connect()

# Links
* [GAO Wiki](https://securewiki.ith.intel.com/display/GAO/Graph+Analytics+Home)
* [JIRA](https://jira01.devtools.intel.com/secure/Dashboard.jspa)
* [TeamCity](https://ubit-teamcity-iag.intel.com/project.html?projectId=Gao)
