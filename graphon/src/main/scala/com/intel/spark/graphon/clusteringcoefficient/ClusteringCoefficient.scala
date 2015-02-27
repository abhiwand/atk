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

package com.intel.spark.graphon.clusteringcoefficient

import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.{ GraphEntity, GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin

case class ClusteringCoefficientArgs(graph: GraphReference,
                                     outputGraphName: Option[String],
                                     outputPropertyName: Option[String],
                                     inputEdgeLabels: Option[List[String]] = None) {
  require((outputGraphName.isEmpty && outputPropertyName.isEmpty) ||
    (outputGraphName.nonEmpty && outputPropertyName.nonEmpty),
    "Either both output_graph_name and output_property_name must be empty or both output_graph_name and output_property_name must be nonempty.")

  def inputEdgeSet: Option[Set[String]] =
    if (inputEdgeLabels.isEmpty) {
      None
    }
    else {
      Some(inputEdgeLabels.get.toSet)
    }
}

/**
 * Result of clustering coefficient calculation.
 * @param globalClusteringCoefficient The global clustering coefficient of the graph.
 * @param graph If local clustering coefficients are requested, a reference to the new graph with local clustering
 *              coefficients stored at properties at each vertex. If local clustering coefficient is not requested,
 *              a reference to the input graph.
 */
case class ClusteringCoefficientResult(globalClusteringCoefficient: Double, graph: GraphEntity)

/** Json conversion for arguments and return value case classes */
object ClusteringCoefficientJsonFormat {
  import com.intel.intelanalytics.domain.DomainJsonProtocol._
  implicit val CCFormat = jsonFormat4(ClusteringCoefficientArgs)
  implicit val CCResultFormat = jsonFormat2(ClusteringCoefficientResult)
}
import ClusteringCoefficientJsonFormat._

/**
 * Calculates the clustering coefficient of the graph with repect to an (optional) set of labels.
 *
 * Pulls graph from underlying store, calculates degrees and writes them into the property specified,
 * and then writes the output graph to the underlying store.
 *
 * Right now it uses only Titan for graph storage. Other backends will be supported later.
 */
class ClusteringCoefficient extends SparkCommandPlugin[ClusteringCoefficientArgs, ClusteringCoefficientResult] {

  override def name: String = "graph:titan/clustering_coefficient"

  override def numberOfJobs(arguments: ClusteringCoefficientArgs)(implicit invocation: Invocation): Int = 6

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: ClusteringCoefficientArgs)(implicit invocation: Invocation): ClusteringCoefficientResult = {

    sc.addJar(SparkContextFactory.jarPath("graphon"))

    // Get the graph

    val graph = engine.graphs.expectGraph(arguments.graph.id)

    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val ccOutput =
      ClusteringCoefficientRunner.run(gbVertices, gbEdges, arguments.outputPropertyName, arguments.inputEdgeSet)

    val outGraph: GraphEntity = if (arguments.outputGraphName.nonEmpty) {
      val newGraphName = arguments.outputGraphName.get
      engine.graphs.writeToTitan(newGraphName, ccOutput.vertices, gbEdges)
    }
    else {
      graph
    }

    ClusteringCoefficientResult(ccOutput.globalClusteringCoefficient, outGraph)
  }

}
