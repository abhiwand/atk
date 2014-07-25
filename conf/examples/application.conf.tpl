# This (application.conf.tpl) is a configuration template for the Intel Analytics Toolkit.
# Copy this to application.conf and edit to suit your system.
# Comments begin with a '#' character.
# Default values are 'commented' out with //.
# To configure for your system, look for configuration entries below with the word
# REQUIRED in all capital letters - these
# MUST be configured for the system to work.

# BEGIN REQUIRED SETTINGS

intel.analytics {

    # The host name for the Postgresql database in which the metadata will be stored
    metastore.connection-postgresql.host = "invalid-postgresql-host"

    engine {

        # The hdfs URL where the intelanalytics folder will be created
        # and which will be used as the starting point for any relative URLs
        fs.root = "hdfs://invalid-fsroot-host/user/iauser"

        # The (comma separated, no spaces) Zookeeper hosts that
        # Titan needs to be able to connect to HBase
        titan.load.hostname = "invalid-titan-host"

        spark {
            # The URL for connecting to the Spark master server
            master = "spark://invalid-spark-master:7077"

            # Memory should be same or lower than what is listed as available in Cloudera Manager.
            # Values should generally be in gigabytes, e.g. "8g"
            spark.executor.memory = "invalid executor memory"
        }
    }

}

# END REQUIRED SETTINGS

# The settings below are all optional. Some may need to be configured depending on the
# specifics of your cluster and workload.

intel.analytics {

  # Configuration for the Intel Analytics REST API server
  api {
      #this is reported by the API server in the /info results - it can be used to identify
      #a particular server or cluster
      //identifier = "ia"

	  #bind address - change to 0.0.0.0 to listen on all interfaces
      //host = "127.0.0.1"

	  #bind port
      //port = 9099

      #The default page size for result pagination
      //default-count = 20

      #Timeout for waiting for results from the engine
      //default-timeout = 30s

      #HTTP request timeout for the api server
      //request-timeout = 29s
    }

	#Connection information for the database where IAT will store its system metadata
	metastore {
	    connection-postgresql {
          //port = 5432
          //database = "metastore"
		  //username = "metastore"
		  //password = "Tribeca123"
	    }

		//connection = ${intel.analytics.metastore.connection-postgresql}
	}

	#Configuration for the IAT processing engine
	engine {
	    //default-timeout = 30s
        //max-rows = 20
	
    spark {

      # When master is empty the system defaults to spark://`hostname`:7070 where hostname is calculated from the current system
      # For local mode (useful only for development testing) set master = "local[4]"
      # in cluster mode, set master and home like the example
      # master = "spark://MASTER_HOSTNAME:7077"
      # home = "/opt/cloudera/parcels/CDH/lib/spark"

      # When home is empty the system will check expected locations on the local system and use the first one it finds
      # ("/usr/lib/spark","/opt/cloudera/parcels/CDH/lib/spark/", etc)
      //home = ""


      # this is the default number of partitions that will be used for RDDs
      default-partitions = 90


      # path to python worker execution, usually to toggle 2.6 and 2.7
      //python-worker-exec = "python" #Other valid values: "python2.7"

      conf {
        properties {
          # These key/value pairs will be parsed dynamically and provided to SparkConf()
          # See Spark docs for possible values http://spark.apache.org/docs/0.9.0/configuration.html
          # All values should be convertible to Strings

          #Examples of other useful properties to edit for performance tuning:

          //spark.akka.frameSize=10000
          //spark.akka.retry.wait=30000
          //spark.akka.timeout=200
          //spark.akka.timeout=30000

          //spark.shuffle.consolidateFiles=true

          //spark.storage.blockManagerHeartBeatMs=300000
          //spark.storage.blockManagerSlaveTimeoutMs=300000

          //spark.worker.timeout=600
          //spark.worker.timeout=30000
        }

      }
    }

    giraph {
      #Overrides of normal Hadoop settings that are used when running Giraph jobs
      //giraph.maxWorkers = 1
      //giraph.minWorkers = 1
      //giraph.SplitMasterWorker = true
      //mapreduce.map.memory.mb = 8192
      //mapreduce.map.java.opts = "-Xmx6554m"
      //giraph.zkIsExternal = false
    }


    titan {
      load {
        # documentation for these settings is available on Titan website
        storage {
          //backend = "hbase"
          //port = "2181"

          #Performance tuning parameters:
          //batch-loading = "true"
          //buffer-size = 2048
          //attempt-wait = 300
          //lock-wait-time = 400
          //lock-retries = 15
          //idauthority-retries = 30
          //read-attempts = 6
          // Pre-split settngs for large datasets
          // region-count = 100
          // short-cf-names = "true"

        }

        //autotype = "none"

        ids {
          //block-size = 300000
          //renew-timeout = 150000
        }
      }
    }
    query {
      storage {
        # query does use the batch load settings in titan.load
        //backend = ${intel.analytics.engine.titan.load.storage.backend}
        //hostname =  ${intel.analytics.engine.titan.load.storage.hostname}
        //port =  ${intel.analytics.engine.titan.load.storage.port}
      }
      cache {
        # Adjust cache size parameters if you experience OutOfMemory errors during Titan queries
        # Either increase heap allocation for IntelAnalytics Engine, or reduce db-cache-size
        # Reducing db-cache will result in cache misses and increased reads from disk
        //db-cache = true
        //db-cache-clean-wait = 20
        //db-cache-time = 180000
		#Allocates 30% of available heap to Titan (default is 50%)
        //db-cache-size = 0.3 
      }
    }
  }
}

