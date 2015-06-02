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

# Developer Todo
* Most items under misc should move to separate repos
* There are a few lingering plugins under engine-core that should move to plugin modules
* Replace Slick with something nicer (even straight JDBC would be better)
* Replace Spray DSL with something nicer (the DSL is confusing to use and our REST layer is too thin to make people learn a DSL)
* Properly support directed and undirected graphs (get rid of "bi-directional")
* Data types in graphs/frames needs to be extended greatly
* Improve Plugins
  * Further separation between framework and plugins (ideally these are even separate projects)
  * Possibly separate plugin from yarn job
  * Dependency Injection
  * Need many more extension points for 3rd parties
  * Meta-programming needs to be expanded to support more kinds of objects
* Integration tests need support added to be able to test Giraph and Titan functions
* testutils should probably merge into engine-core
* giraph-plugins needs refactoring of packages (the current hierarchy is very poorly organized)
* Need Maven profiles to make it easier for developers to build only part of the project

# Links
* [GAO Wiki](https://securewiki.ith.intel.com/display/GAO/Graph+Analytics+Home)
* [JIRA](https://jira01.devtools.intel.com/secure/Dashboard.jspa)
* [TeamCity](https://ubit-teamcity-iag.intel.com/project.html?projectId=Gao)
