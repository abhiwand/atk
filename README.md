Intel Analytics Toolkit (ATK)
=============================

# Building
* Install Python dependencies
* Run 'mvn install' for dependencies found under misc (event, event-api, titan-shaded)
* Run 'mvn install' at top level

# Folders Overview
* bin/ - script for starting REST server, gremlin shell
* conf/ - configuration templates for setting up a system, put your application.conf here for running out of source_code
* doc/ - end user docs for using the system (doc for specific plugins goes elsewhere)
* doc-api-examples/ - examples of api usage for end user docs
* engine/ - loads and executes plugins and provides the basic services that plugins need.
* engine-interfaces/ - interfaces the Engine exposes to the REST server. (we should move plugin args/return values out of here)
* giraph-plugins/ - a few graph algorithms that run on Giraph
* graphbuilder - Titan graph construction and reading
* graphon/ - some graph related plugins that run on Spark and GraphX
* integration-tests/ - developer written, build time integration tests in python, these run against a minimal version of our product
* launcher/ - starts up our application, launches parts of our app
* meta-store/ - code that interacts with the meta-store database repository including SQL scripts
* misc/ - miscellaneous items that aren't really part of our product
* package/ - packaging for VM's, RPM's
* python/ - python client code
* python-examples/ - internal example code, add examples here so we don't have to email scripts around
* rest-server/ - the rest server converts HTTP requests to Akka messages that are sent to the Engine
* testutils/ - some test utility code that gets reused between tests in different modules

# Links
* [GAO Wiki](https://securewiki.ith.intel.com/display/GAO/Graph+Analytics+Home)
* [JIRA](https://jira01.devtools.intel.com/secure/Dashboard.jspa)
* [TeamCity](https://ubit-teamcity-iag.intel.com/project.html?projectId=Gao)
