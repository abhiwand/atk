package com.intel.graphbuilder.driver.spark

import com.intel.testutils.{ DirectoryUtils, LogUtils }
import java.io.File
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.thinkaurelius.titan.core.TitanGraph

/**
 * This trait can be mixed into Tests to get a TitanGraph backed by Berkeley for testing purposes.
 *
 * IMPORTANT! only one thread can use the graph below at a time. This isn't normally an issue because
 * each test usually gets its own copy.
 *
 * IMPORTANT! You must call cleanupTitan() when you are done
 */
trait TestingTitan {

  private var tmpDir: File = null

  var titanConfig: SerializableBaseConfiguration = null
  var titanConnector: TitanGraphConnector = null
  var graph: TitanGraph = null

  def setupTitan(): Unit = {
    LogUtils.silenceTitan()

    tmpDir = DirectoryUtils.createTempDirectory("titan-graph-for-unit-testing-")

    titanConfig = new SerializableBaseConfiguration()
    titanConfig.setProperty("storage.backend", "berkeleyje")
    titanConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

    titanConnector = new TitanGraphConnector(titanConfig)
    graph = titanConnector.connect()
  }

  /**
   * IMPORTANT! removes temporary files
   */
  def cleanupTitan(): Unit = {
    try {
      if (graph != null) {
        graph.commit()

        //graph.shutdown()
      }
    }
    finally {
      DirectoryUtils.deleteTempDirectory(tmpDir)
    }

    // make sure this class is unusable when we're done
    titanConfig = null
    titanConnector = null
    graph = null
    tmpDir = null
  }

}
