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

import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.{ StorageFormats, DomainJsonProtocol }
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.UUID
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._

import scala.concurrent.Await

case class AnnotateDegreesArgs(graph: GraphReference,
                               outputGraphName: String,
                               outputPropertyName: String,
                               degreeOption: Option[String] = None,
                               inputEdgeLabels: Option[List[String]] = None) {
  require(!outputPropertyName.isEmpty, "Output property label must be provided")
  require(!outputGraphName.isEmpty, "Output graph name must be provided")
}

/**
 * The result object
 * @param graph Name of the output graph
 */
case class AnnotateDegreesResult(graph: String)

/** Json conversion for arguments and return value case classes */
object AnnotateDegreesJsonFormat {
  import DomainJsonProtocol._
  implicit val ADFormat = jsonFormat5(AnnotateDegreesArgs)
  implicit val ADResultFormat = jsonFormat1(AnnotateDegreesResult)
}

import AnnotateDegreesJsonFormat._

/**
 * Calculates the degree of each vertex with repect to an (optional) set of labels.
 *
 *
 *
 * Pulls graph from underlying store, calculates degrees and writes them into the property specified,
 * and then writes the output graph to the underlying store.
 *
 * Right now it uses only Titan for graph storage. Other backends will be supported later.
 */
class AnnotateDegrees extends SparkCommandPlugin[AnnotateDegreesArgs, AnnotateDegreesResult] {

  override def name: String = "graph:titan/annotate_degrees"

  override def numberOfJobs(arguments: AnnotateDegreesArgs)(implicit invocation: Invocation): Int = 4

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: AnnotateDegreesArgs)(implicit invocation: Invocation): AnnotateDegreesResult = {

    sc.addJar(SparkContextFactory.jarPath("graphon"))

    // Titan Settings for input
    val config = configuration

    // validate arguments

    val newGraphName = arguments.outputGraphName
    require(newGraphName != "", "output graph name cannot be empty")
    val outputPropertyName = arguments.outputPropertyName
    require(outputPropertyName != "", "output property name cannot be empty")

    val degreeMethod: String = arguments.degreeOption.getOrElse("out")
    require("out".startsWith(degreeMethod) || "in".startsWith(degreeMethod) || "undirected".startsWith(degreeMethod),
      "degreeMethod should be prefix of 'in', 'out' or 'undirected', not " + degreeMethod)

    // Get the graph
    import scala.concurrent.duration._
    val graph = Await.result(engine.getGraph(arguments.graph.id), config.getInt("default-timeout") seconds)

    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)
    gbVertices.persist(StorageLevel.MEMORY_AND_DISK_SER)
    gbEdges.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val inputEdgeLabels: Option[Set[String]] =
      if (arguments.inputEdgeLabels.isEmpty) {
        None
      }
      else {
        Some(arguments.inputEdgeLabels.get.toSet)
      }

    val vertexDegreePairs: RDD[(GBVertex, Long)] = if ("undirected".startsWith(degreeMethod)) {
      DegreeStatistics.undirectedDegreesByEdgeLabel(gbVertices, gbEdges, inputEdgeLabels)
    }
    else if ("in".startsWith(degreeMethod)) {
      DegreeStatistics.inDegreesByEdgeLabel(gbVertices, gbEdges, inputEdgeLabels)
    }
    else {
      DegreeStatistics.outDegreesByEdgeLabel(gbVertices, gbEdges, inputEdgeLabels)
    }

    val outEdges = gbEdges
    val outVertices = vertexDegreePairs.map({
      case (v: GBVertex, d: Long) => GBVertex(physicalId = v.physicalId,
        gbId = v.gbId,
        properties = v.properties + Property(outputPropertyName, d))
    })

    val newGraph = Await.result(engine.createGraph(GraphTemplate(Some(newGraphName), StorageFormats.HBaseTitan)),
      config.getInt("default-timeout") seconds)

    // create titan config copy for newGraph write-back
    val newTitanConfig = GraphBuilderConfigFactory.getTitanConfiguration(newGraph.name.get)
    writeToTitan(newTitanConfig, outVertices, outEdges)

    gbVertices.unpersist()
    gbEdges.unpersist()

    AnnotateDegreesResult(newGraphName)
  }

  // Helper function to write rdds back to Titan
  private def writeToTitan(titanConfig: SerializableBaseConfiguration, gbVertices: RDD[GBVertex], gbEdges: RDD[GBEdge], append: Boolean = false) = {
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append))
    gb.buildGraphWithSpark(gbVertices, gbEdges)
  }

}
