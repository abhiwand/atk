Intel Analytics Toolkit (ATK)
=============================

# Building
* Install Python dependencies
* Run 'mvn install' for dependencies found under misc (event, event-api, titan-shaded)
* Run 'mvn install' at top level

# Folders Overview
* doc-api-examples/ - customer facing plugin docs
* bin/ - script for starting REST server, gremlin shell
* conf/ - configuration templates for setting up a system, put your application.conf here for running out of source_code
* doc/ - customer facing docs
* engine/ - engine interface (seems like we should just delete this module)
* engine-spark - majority of the engine code
* giraph-plugins/ - a few graph algorithms that run on Giraph
* graphbuilder - Titan graph construction and reading
* graphon/ - some graph related plugins that run on Spark and GraphX
* integration-tests/ - developer written, build time integration tests in python, these run against a minimal version of our product
* interfaces/ - interfaces shared between rest-server and engine-spark (we should move plugin args/return values out of here)
* launcher/ - starts up our application, launches parts of our app
* meta-store/ - code that interacts with the meta-store database repository including SQL scripts
* misc/ - miscellaneous items that aren't really part of our product
* package/ - packaging for VM's, RPM's
* python/ - python client code
* python-examples/ - internal example code, add examples here so we don't have to email scripts around
* rest-server/ - the rest server
* testutils/ - some test utility code that get re-used between tests in different modules

# Links
* [GAO Wiki](https://securewiki.ith.intel.com/display/GAO/Graph+Analytics+Home)
* [JIRA](https://jira01.devtools.intel.com/secure/Dashboard.jspa)
* [TeamCity](https://ubit-teamcity-iag.intel.com/project.html?projectId=Gao)
