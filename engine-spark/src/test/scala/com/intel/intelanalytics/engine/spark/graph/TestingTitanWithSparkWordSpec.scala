package com.intel.intelanalytics.engine.spark.graph

import java.io.File

import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.testutils.{ TestingSparkContextWordSpec, DirectoryUtils, LogUtils }
import com.thinkaurelius.titan.core.util.TitanCleanup
import com.thinkaurelius.titan.core.{ TitanFactory, TitanGraph }
import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph
import org.scalatest.BeforeAndAfter

trait TestingTitanWithSparkWordSpec extends TestingSparkContextWordSpec with BeforeAndAfter {
  LogUtils.silenceTitan()

  var tmpDir: File = null
  var titanGraph: TitanGraph = null
  var graph: Graph = null
  val titanConfig = new SerializableBaseConfiguration()

  override def beforeAll() {
    super[TestingSparkContextWordSpec].beforeAll()
    tmpDir = DirectoryUtils.createTempDirectory("engine-tmp-dir-for-titan-tests")

    titanConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

    titanGraph = TitanFactory.open(titanConfig)
    graph = new IdGraph(titanGraph, true, false)
  }

  override def afterAll() {
    super[TestingSparkContextWordSpec].afterAll()
    cleanupTitan()
  }

  /**
   * IMPORTANT! Closes in-memory graph used for testing
   */
  def cleanupTitan(): Unit = {
    try {
      if (graph != null) {
        graph.shutdown()
      }
    }
    finally {
      TitanCleanup.clear(titanGraph)

      DirectoryUtils.deleteTempDirectory(tmpDir)

      tmpDir = null
      titanGraph = null
      graph = null
    }

  }

}
