package com.intel.graphbuilder.testutils

import DirectoryUtils._
import com.intel.graphbuilder.graph.titan.TitanGraphConnector
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.thinkaurelius.titan.core.TitanGraph
import java.io.File
import org.specs2.mutable.BeforeAfter

/**
 * This trait can be mixed into Specifications to get a TitanGraph backed by Berkeley for testing purposes.
 *
 * IMPORTANT! only one thread can use the graph below at a time. This isn't normally an issue because
 * each test usually gets its own copy.
 */
trait TestingTitan extends MultipleAfter {

  LogUtils.silenceTitan()

  private var tmpDir: File = createTempDirectory("titan-graph-for-unit-testing-")

  var titanConfig = new SerializableBaseConfiguration()
  titanConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

  var titanConnector = new TitanGraphConnector(titanConfig)
  var graph: TitanGraph = titanConnector.connect()

  override def after: Unit = {
    cleanupTitan()
    super.after
  }

  /**
   * IMPORTANT! removes temporary files
   */
  def cleanupTitan(): Unit = {
    try {
      if (graph != null) {
        graph.shutdown()
      }
    }
    finally {
      deleteTempDirectory(tmpDir)
    }

    // make sure this class is unusable when we're done
    titanConfig = null
    titanConnector = null
    graph = null
    tmpDir = null
  }

}
