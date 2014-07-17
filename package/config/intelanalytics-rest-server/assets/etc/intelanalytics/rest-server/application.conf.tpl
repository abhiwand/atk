intel.analytics {
  api {
      identifier = "ia"
      host = "127.0.0.1"
      port = 9099
      defaultCount = 20
      defaultTimeout = 30
    }

  component.archives {
    engine {class = "com.intel.intelanalytics.engine.EngineApplication"}
    igiraph-titan {}
  }

  engine {
    plugin {
      command {
        archives = ["engine-spark", "igiraph-titan", "graphon"]
      }
    }

    default-timeout = 30
    max-rows = 20

    fs {
      # the system will create an "intelanalytics" folder at this location, if set,
      # or at the root of the filesystem, if not. All Intel Analytics Toolkit files will
      # be stored somehwere under that base location.
      #
      # For example, if using HDFS, set the root to hdfs path
      # root = "hdfs://MASTER_HOSTNAME/some/path"
      #
      # If running in local mode, this might be a better choice:
      # root = ${user.home}
      root = "hdfs://master/user/hadoop"
    }

    spark {

      // When master is empty the system defaults to spark://`hostname`:7070 where hostname is calculated from the current system
      master = ""
      // When home is empty the system will check expected locations on the local system and use the first one it finds
      // ("/usr/lib/spark","/opt/cloudera/parcels/CDH/lib/spark/", etc)
      home = ""

      // in cluster mode, set master and home like the example
      // master = "spark://MASTER_HOSTNAME:7077"
      // home = "/opt/cloudera/parcels/CDH/lib/spark"
      // home = "/usr/lib/spark"

      // local mode
      // master = "local[4]"

      // this is the default number of partitions that will be used for RDDs
      defaultPartitions = 90


      // path to python worker execution, usually to toggle 2.6 and 2.7
      //pythonWorkerExec = "python2.7"
      pythonWorkerExec = "python"

      conf {
        properties {
          // These key/value pairs will be parsed dynamica2ostlly and provided to SparkConf()
          // See Spark docs for possible values http://spark.apache.org/docs/0.9.0/configuration.html
          // All values should be convertible to Strings

          //spark.akka.frameSize=10000
          //spark.akka.retry.wait=30000
          //spark.akka.timeout=200
          //spark.akka.timeout=30000

          // Memory should be same or lower than what is listed as available in Cloudera Manager
          //spark.executor.memory = "16g"
          spark.executor.memory = "8g"

          //spark.shuffle.consolidateFiles=true

          //spark.storage.blockManagerHeartBeatMs=300000
          //spark.storage.blockManagerSlaveTimeoutMs=300000

          //spark.worker.timeout=600
          //spark.worker.timeout=30000
        }

      }
    }

    #This section provides overrides to the default Hadoop configuration
    hadoop {
      #The path from which to load base configurations (e.g. core-site.xml would be in this folder)
      configuration.path = "ignore/etc/hadoop/conf"
      mapreduce {
        job.user.classpath.first = true
        framework.name = "yarn"
      }
      #yarn.resourcemanager.address  = "master:8032"
    }


    giraph = ${intel.analytics.engine.hadoop}
    giraph {
      #Overrides of normal Hadoop settings that are used when running Giraph jobs
      giraph.maxWorkers = 1
      giraph.minWorkers = 1
      giraph.SplitMasterWorker = true
      mapreduce.map.memory.mb = 8192
      mapreduce.map.java.opts = "-Xmx8g"
      giraph.zkIsExternal = false
      mapred.job.tracker = "not used" #not used - this can be set to anything but 'local' to make Giraph work
      archive.name = "igiraph-titan" #name of the plugin jar (without suffix) to launch
    }

    commands {
      dataframes.create = {}
      graphs.ml {
        //      als {
        //        giraph = ${intel.analytics.engine.giraph}
        //        giraph {
        //          //overrides of normal giraph settings for this algo go here
        //        }
        //        convergence-threshold = 0
        //        lambda = 0.065
        //        feature-dimension = 3
        //        bias-on = true
        //        learning-curve-output-interval = 1
        //        max-val = "Infinity"
        //        min-val = "-Infinity"
        //        bidirectional-check = true
        //        max-supersteps = 10
        //      }
      }
      graphs.query {}
    }

    //  query {
    //    ALSQuery {
    //      key-name = "id"
    //      vertex-type-name = "vertex_type"
    //      bias-on = true
    //      edge-type-name = "edge_type"
    //      edge-type = "edge"
    //      feature-dimensions = 1
    //      left-type = "L"
    //      right-type = "R"
    //      left-name = "user"
    //      right-name = "item"
    //      result-property-list = "als_p0;als_p1;als_p3;als_bias"
    //      train = "TR"
    //    }
    //  }

    titan {
      load {
        // documentation for these settings is available on Titan website
        storage {
          backend = "hbase"
          // with clusters the hostname should be a comma separated list of host names with zookeeper role assigned
          hostname = "master"
          port = "2181"
          batch-loading = "true"
          buffer-size = 2048
          attempt-wait = 300
          lock-wait-time = 400
          lock-retries = 15
          idauthority-retries = 30
          read-attempts = 6
          // Pre-split settngs for large datasets
          // region-count = 100
          // short-cf-names = "true"

        }

        autotype = "none"

        ids {
          block-size = 300000
          renew-timeout = 150000
        }
      }
    }
    query {
      storage {
        // query does use the batch load settings in titan.load
        // TODO: should these variables be under intel.analytics.engine.titan or is this ok?
        backend = ${intel.analytics.engine.titan.load.storage.backend}
        hostname =  ${intel.analytics.engine.titan.load.storage.hostname}
        port =  ${intel.analytics.engine.titan.load.storage.port}
      }
      cache {
        // Adjust cache size parameters if you experience OutOfMemory errors during Titan queries
        // Either increase heap allocation for IntelAnalytics Engine, or reduce db-cache-size
        // Reducing db-cache will result in cache misses and increased reads from disk
        db-cache = true
        db-cache-clean-wait = 20
        db-cache-time = 180000
        db-cache-size = 0.3 //Allocates 30% of available heap to Titan (default is 50%)
      }
    }
  }
}

