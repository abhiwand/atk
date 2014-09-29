package com.intel.testutils

import java.io.File

import com.thinkaurelius.titan.core.{ TitanFactory, TitanGraph }
import com.tinkerpop.blueprints.{ Vertex, Graph }
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph
import org.apache.commons.configuration.BaseConfiguration

/**
 * This trait can be mixed into Tests to get a TitanGraph backed by Berkeley for testing purposes.
 *
 * The TitanGraph can be wrapped by IdGraph to allow tests to create vertices and edges with specific Ids.
 *
 * IMPORTANT! only one thread can use the graph below at a time. This isn't normally an issue because
 * each test usually gets its own copy.
 *
 * IMPORTANT! You must call cleanupTitan() when you are done
 */

trait TestingTitan {
  LogUtils.silenceTitan()

  var tmpDir: File = null
  var titanBaseConfig: BaseConfiguration = null
  var graph: TitanGraph = null
  var idGraph: Graph = null

  def setupTitan(): Unit = {
    tmpDir = DirectoryUtils.createTempDirectory("titan-graph-for-unit-testing-")

    titanBaseConfig = new BaseConfiguration()
    titanBaseConfig.setProperty("storage.backend", "berkeleyje")
    titanBaseConfig.setProperty("storage.directory", tmpDir.getAbsolutePath)

    graph = TitanFactory.open(titanBaseConfig)
    idGraph = setupIdGraph()
    println(idGraph)
  }

  def setupIdGraph(): IdGraph[TitanGraph] = {
    val graphManager = graph.getManagementSystem()
    val idKey = graphManager.makePropertyKey(IdGraph.ID).dataType(classOf[java.lang.Long]).make()
    graphManager.buildIndex(IdGraph.ID, classOf[Vertex]).addKey(idKey).buildCompositeIndex()
    graphManager.commit()
    new IdGraph(graph, true, false)
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
    titanBaseConfig = null
    graph = null
    tmpDir = null
    idGraph = null
  }

}
