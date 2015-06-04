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

package com.intel.spark.graphon.graphstatistics

import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.util.SerializableBaseConfiguration
import com.intel.intelanalytics.domain.frame.{ FrameMeta, FrameEntity }
import com.intel.intelanalytics.domain.{ CreateEntityArgs, StorageFormats, DomainJsonProtocol }
import com.intel.intelanalytics.domain.graph.{ GraphEntity, GraphTemplate, GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import com.intel.intelanalytics.engine.spark.graph._
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.UUID
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

import scala.concurrent.Await

case class AnnotateDegreesArgs(graph: GraphReference,
                               @ArgDoc("") outputPropertyName: String,
                               @ArgDoc("") degreeOption: Option[String] = None,
                               @ArgDoc("") inputEdgeLabels: Option[List[String]] = None) {
  require(!outputPropertyName.isEmpty, "Output property label must be provided")

  // validate arguments

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

  def useUndirected() = "undirected".startsWith(degreeMethod)
  def useInDegree() = "in".startsWith(degreeMethod)
  def useOutDegree() = "out".startsWith(degreeMethod)
}

case class AnnotateDegreesReturn(frameDictionaryOutput: Map[String, FrameEntity])

import DomainJsonProtocol._
import spray.json._

/** Json conversion for arguments and return value case classes */
object AnnotateDegreesJsonFormat {

  implicit val ADArgsFormat = jsonFormat4(AnnotateDegreesArgs)
  implicit val ADReturnFormat = jsonFormat1(AnnotateDegreesReturn)
}

import AnnotateDegreesJsonFormat._

@PluginDoc(oneLine = "Calculates the degree of each vertex with respect to an (optional) set of labels.",
  extended = """Pulls graph from underlying store, calculates degrees and writes them into the property specified,
and then writes the output graph to the underlying store.

Right now it uses only Titan for graph storage. Other backends will be supported later.""")
class AnnotateDegreesPlugin extends SparkCommandPlugin[AnnotateDegreesArgs, AnnotateDegreesReturn] {

  override def name: String = "graph/annotate_degrees"

  override def numberOfJobs(arguments: AnnotateDegreesArgs)(implicit invocation: Invocation): Int = 4

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: AnnotateDegreesArgs)(implicit invocation: Invocation): AnnotateDegreesReturn = {

    val degreeMethod: String = arguments.degreeMethod
    // Get the graph
    import scala.concurrent.duration._

    val graph = engine.graphs.expectGraph(arguments.graph)

    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val inputEdgeSet = arguments.inputEdgeSet

    val vertexDegreePairs: RDD[(GBVertex, Long)] = if (arguments.useUndirected()) {
      UnweightedDegrees.undirectedDegreesByEdgeLabel(gbVertices, gbEdges, inputEdgeSet)
    }
    else if (arguments.useInDegree()) {
      UnweightedDegrees.inDegreesByEdgeLabel(gbVertices, gbEdges, inputEdgeSet)
    }
    else {
      UnweightedDegrees.outDegreesByEdgeLabel(gbVertices, gbEdges, inputEdgeSet)
    }

    val outVertices = vertexDegreePairs.map({
      case (v: GBVertex, d: Long) => GBVertex(physicalId = v.physicalId,
        gbId = v.gbId,
        properties = v.properties + Property(arguments.outputPropertyName, d))
    })

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)

    new AnnotateDegreesReturn(frameRddMap.keys.map(label => {
      val result = tryNew(CreateEntityArgs(description = Some("created by annotated degrees operation"))) { newOutputFrame: FrameMeta =>
        val frameRdd = frameRddMap(label)
        save(new SparkFrameData(newOutputFrame.meta, frameRdd))
      }.meta
      (label, result)
    }).toMap)

  }
}
