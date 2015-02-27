source_code
===========

# Folders / modules overview
* api-doc/ - customer facing plugin docs
* api-server/ - the rest server
* bin/ - script for starting api server, gremlin shell
* conf/ - configuration templates for setting up a system, put your application.conf here for running out of source_code
* doc/ - customer facing docs
* engine/ - engine interface (seems like we should just delete this module)
* engine-spark - majority of the engine code
* event/ - logging framework
* event-api/ - logging framework
* examples/ - internal example code, add examples here so we don't have to email scripts around
* graphbuilder-3 - Titan graph construction and reading
* graphon/ - some graph related plugins that run on Spark and GraphX
* igiraph/ - a few graph algorithms that run on Giraph
* imahout/ - a couple of files someone thought we might contribute to mahout - module should be deleted
* integration-tests/ - developer written, build time integration tests in python, these run against a minimal version of our product
* interfaces/ - interfaces shared between api-server and engine-spark (we should move plugin args/return values out of here)
* ispark-deps/ - was needed for getting around some classloader issues - module can probably be deleted
* ititan/ - repackaging of Titan to get around classloader issues
* launcher/ - starts up our application, launches parts of our app
* package/ - packaging for VM's, RPM's
* python/ - python client code
* scala-coverage/ - some scripts for getting scala code coverage numbers
* scripts/ - RPM packaging
* shared/ - mostly code for our meta-data repository, SQL scripts for our meta-data repository
* testutils/ - some test utility code that get re-used between tests in different modules
* tools/ - tools that aren't really part of our product


# Links
* [GAO Wiki](https://securewiki.ith.intel.com/display/GAO/Graph+Analytics+Home)
* [JIRA](https://jira01.devtools.intel.com/secure/Dashboard.jspa)
* [TeamCity](https://ubit-teamcity-iag.intel.com/viewType.html?buildTypeId=Tribeca_SourceCodeTest)
