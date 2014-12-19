
This folder is for integration tests that are run at build time.

- Runs out of source_code and assumes jars have been built using 'mvn package'
- Spark is ran in local mode
- No HDFS, everything is against local file system
- No Titan available, yet (maybe Berkeley later)
- No PostgreSQL, all tests are against in-memory H2 DB
- All output should go to 'target' folder and using in-memory DB - so no clean-up is needed in tests
- You can view the data left behind from a test

-------------------
  Files
-------------------
/integration-tests
    /datasets - folder for data sets, these will be copied to fs root
    /target - folder for all generated files, fs root gets created under here
    /target
        /fs-root - folder that functions like the HDFS root used by our application
            /datasets - copy of top-level /datasets folder
            /intelanalytics - frames and graphs get written here
        /api-server.log - output from API Server
        /api-server.pid - contains PID of API Server so we can kill it when we're done
    /testcases - pyunit tests
    /api-server-start.sh - script for starting API server (you don't need to call directly)
    /api-server-stop.sh - script for stopping API server (that was started with the start script)
    /clean.sh - script for removing generated files (removes everything under target folder)
    /README.txt - this text file
    /run_tests.sh - script for running tests (start server, run tests, stop server)

-------------------
  Instructions
-------------------
- Please only add the absolutely smallest toy data sets!
- Remember these are build-time tests, we need to have them run as fast as possible
- Avoid outside dependencies (Spark, HDFS, etc.), these tests should be self-contained so they can easily run on any build server
- Preferably we treat data sets as immutable

-------------------
  TODO
-------------------
- Allow tests to go against Titan
- Modify logging to go under target/logs
- Parallel execution seems to be limited by Spray and Akka right now

