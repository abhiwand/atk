//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

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
