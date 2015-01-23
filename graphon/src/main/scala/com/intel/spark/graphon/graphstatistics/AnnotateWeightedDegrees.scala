////////////////////////////////////////////////////////////////////////////////
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
////////////////////////////////////////////////////////////////////////////////

package com.intel.spark.graphon.graphstatistics

import com.intel.graphbuilder.elements.{ GBVertex, Property }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

case class AnnotateWeightedDegreesArgs(graph: GraphReference,
                                       outputGraphName: String,
                                       outputPropertyName: String,
                                       degreeOption: Option[String] = None,
                                       inputEdgeLabels: Option[List[String]] = None,
                                       edgeWeightProperty: Option[String] = None,
                                       edgeWeightDefault: Option[Double] = None) {

  // validate arguments

  require(!outputPropertyName.isEmpty, "Output property label must be provided")
  require(!outputGraphName.isEmpty, "Output graph name must be provided")
  def degreeMethod: String = degreeOption.getOrElse("out")
  require("out".startsWith(degreeMethod) || "in".startsWith(degreeMethod) || "undirected".startsWith(degreeMethod),
    "degreeMethod should be prefix of 'in', 'out' or 'undirected', not " + degreeMethod)

  def inputEdgeSet: Option[Set[String]] =
    if (inputEdgeLabels.isEmpty) {
      None
    }
    else {
      Some(inputEdgeLabels.get.toSet)
    }

  def getDefaultEdgeWeight(): Double = {
    if (edgeWeightProperty.isEmpty) {
      1.0d
    }
    else {
      edgeWeightDefault.getOrElse(1.0d)
    }
  }
}

/**
 * The result object
 * @param graph Name of the output graph
 */
case class AnnotateWeightedDegreesResult(graph: String)

/** Json conversion for arguments and return value case classes */
object AnnotateWeightedDegreesJsonFormat {
  import DomainJsonProtocol._
  implicit val AWDFormat = jsonFormat7(AnnotateWeightedDegreesArgs)
  implicit val AWDResultFormat = jsonFormat1(AnnotateWeightedDegreesResult)
}

import AnnotateWeightedDegreesJsonFormat._

/**
 * Calculates the weighted degree of each vertex with respect to an (optional) set of labels.
 *
 *
 *
 * Pulls graph from underlying store, calculates weighted degrees and writes them into the property specified,
 * and then writes the output graph to the underlying store.
 *
 * Right now it uses only Titan for graph storage. Other backends will be supported later.
 */
class AnnotateWeightedDegrees extends SparkCommandPlugin[AnnotateWeightedDegreesArgs, AnnotateWeightedDegreesResult] {

  override def name: String = "graph:titan/annotate_weighted_degrees"

  override def numberOfJobs(arguments: AnnotateWeightedDegreesArgs)(implicit invocation: Invocation): Int = 4

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: AnnotateWeightedDegreesArgs)(implicit invocation: Invocation): AnnotateWeightedDegreesResult = {

    sc.addJar(SparkContextFactory.jarPath("graphon"))

    val degreeMethod: String = arguments.degreeMethod
    val newGraphName = arguments.outputGraphName

    // Get the graph

    val graph = engine.graphs.expectGraph(arguments.graph.id)

    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)
    gbVertices.persist(StorageLevel.MEMORY_AND_DISK_SER)
    gbEdges.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val inputEdgeSet = arguments.inputEdgeSet
    val weightPropertyOPtion = arguments.edgeWeightProperty
    val defaultEdgeWeight = arguments.getDefaultEdgeWeight()

    val vertexWeightPairs: RDD[(GBVertex, Double)] = if ("undirected".startsWith(degreeMethod)) {
      WeightedDegrees.undirectedDegreesByEdgeLabel(gbVertices, gbEdges, weightPropertyOPtion, defaultEdgeWeight, inputEdgeSet)
    }
    else if ("in".startsWith(degreeMethod)) {
      WeightedDegrees.inDegreesByEdgeLabel(gbVertices, gbEdges, weightPropertyOPtion, defaultEdgeWeight, inputEdgeSet)
    }
    else {
      WeightedDegrees.outDegreesByEdgeLabel(gbVertices, gbEdges, weightPropertyOPtion, defaultEdgeWeight, inputEdgeSet)
    }

    val outVertices = vertexWeightPairs.map({
      case (v: GBVertex, wd: Double) => GBVertex(physicalId = v.physicalId,
        gbId = v.gbId,
        properties = v.properties + Property(arguments.outputPropertyName, wd))
    })

    engine.graphs.writeToTitan(newGraphName, outVertices, gbEdges)
    gbVertices.unpersist()
    gbEdges.unpersist()

    AnnotateWeightedDegreesResult(newGraphName)
  }
}
