//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
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

package com.intel.spark.graphon.hierarchicalclustering

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.graph.{ GraphNoArgs, GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 *
 * @param graph
 */
case class HierarchicalClusteringArgs(graph: GraphReference,
                                      @ArgDoc("""Column name for the edge distance.""") edgeDistance: String)

/** Json conversion for arguments and return value case classes */
object HierarchicalClusteringFormat {
  import DomainJsonProtocol._
  implicit val hFormat = jsonFormat2(HierarchicalClusteringArgs)
}

import HierarchicalClusteringFormat._

/**
 * HierarchicalClusteringPlugin implements the hierarchical clustering algorithm on a graph.
 */
@PluginDoc(oneLine = "Build hierarchical clustering over an initial titan graph.",
  extended = "",
  returns = """A set of titan vertices and edges representing the internal clustering of the graph.""")
class HierarchicalClusteringPlugin extends SparkCommandPlugin[HierarchicalClusteringArgs, UnitReturn] {

  override def name: String = "graph:titan/hierarchical_clustering"
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: HierarchicalClusteringArgs)(implicit invocation: Invocation): UnitReturn = {

    if (!SparkEngineConfig.isSparkOnYarn)
      sc.addJar(SparkContextFactory.jarPath("graph-plugins"))
    val graph = engine.graphs.expectGraph(arguments.graph)
    val (vertices, edges) = engine.graphs.loadGbElements(sc, graph)
    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)

    new HierarchicalClusteringWorker(titanConfig).execute(vertices, edges, arguments.edgeDistance)
    new UnitReturn
  }
}
