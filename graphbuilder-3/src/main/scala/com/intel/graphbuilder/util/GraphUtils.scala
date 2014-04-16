package com.intel.graphbuilder.util

import com.tinkerpop.blueprints.Graph
import scala.collection.JavaConversions._


/**
 * Utility methods for Graphs that don't fit better else where
 */
object GraphUtils {

  /**
   * Dump the entire graph into a String (not scalable obviously but nice for quick testing)
   */
  def dumpGraph(graph: Graph): String = {
    var vertexCount = 0
    var edgeCount = 0

    val output = new StringBuilder("---- Graph Dump ----\n")

    graph.getVertices.toList.foreach(v => {
      output.append(v).append("\n")
      vertexCount += 1
    })

    graph.getEdges.toList.foreach(e => {
      output.append(e).append("\n")
      edgeCount += 1
    })

    output.append(vertexCount + " Vertices, " + edgeCount + " Edges")

    output.toString()
  }
}
