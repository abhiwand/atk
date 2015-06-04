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

import com.intel.graphbuilder.elements.{ GBVertex, Property }
import com.intel.intelanalytics.domain.frame.{ FrameMeta, FrameEntity }
import com.intel.intelanalytics.domain.{ CreateEntityArgs, StorageFormats, DomainJsonProtocol }
import com.intel.intelanalytics.domain.graph.{ GraphTemplate, GraphEntity, GraphReference }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

case class AnnotateWeightedDegreesArgs(graph: GraphReference,
                                       @ArgDoc("") outputPropertyName: String,
                                       @ArgDoc("") degreeOption: Option[String] = None,
                                       @ArgDoc("") inputEdgeLabels: Option[List[String]] = None,
                                       @ArgDoc("") edgeWeightProperty: Option[String] = None,
                                       @ArgDoc("") edgeWeightDefault: Option[Double] = None) {

  // validate arguments

  require(!outputPropertyName.isEmpty, "Output property label must be provided")
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

  def getDefaultEdgeWeight(): Double = {
    if (edgeWeightProperty.isEmpty) {
      1.0d
    }
    else {
      edgeWeightDefault.getOrElse(1.0d)
    }
  }
}

case class AnnotateWeightedDegreesReturn(frameDictionaryOutput: Map[String, FrameEntity])

import DomainJsonProtocol._
import spray.json._

/** Json conversion for arguments and return value case classes */
object AnnotateWeightedDegreesJsonFormat {

  implicit val AWDArgsFormat = jsonFormat6(AnnotateWeightedDegreesArgs)
  implicit val AWDReturnFormat = jsonFormat1(AnnotateWeightedDegreesReturn)
}

import AnnotateWeightedDegreesJsonFormat._

@PluginDoc(oneLine = "Calculates the weighted degree of each vertex with respect to an (optional) set of labels.",
  extended = """Pulls graph from underlying store, calculates weighted degrees and writes them into the property
specified, and then writes the output graph to the underlying store.

Right now it uses only Titan for graph storage. Other backends will be supported later.""")
class AnnotateWeightedDegreesPlugin extends SparkCommandPlugin[AnnotateWeightedDegreesArgs, AnnotateWeightedDegreesReturn] {

  override def name: String = "graph/annotate_weighted_degrees"

  override def numberOfJobs(arguments: AnnotateWeightedDegreesArgs)(implicit invocation: Invocation): Int = 4

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: AnnotateWeightedDegreesArgs)(implicit invocation: Invocation): AnnotateWeightedDegreesReturn = {

    val degreeMethod: String = arguments.degreeMethod
    // Get the graph

    val graph = engine.graphs.expectGraph(arguments.graph)

    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val inputEdgeSet = arguments.inputEdgeSet
    val weightPropertyOPtion = arguments.edgeWeightProperty
    val defaultEdgeWeight = arguments.getDefaultEdgeWeight()

    val vertexWeightPairs: RDD[(GBVertex, Double)] = if (arguments.useUndirected()) {
      WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(gbVertices, gbEdges, weightPropertyOPtion, defaultEdgeWeight, inputEdgeSet)
    }
    else if (arguments.useInDegree()) {
      WeightedDegrees.inWeightByEdgeLabel(gbVertices, gbEdges, weightPropertyOPtion, defaultEdgeWeight, inputEdgeSet)
    }
    else {
      WeightedDegrees.outDegreesByEdgeLabel(gbVertices, gbEdges, weightPropertyOPtion, defaultEdgeWeight, inputEdgeSet)
    }

    val outVertices = vertexWeightPairs.map({
      case (v: GBVertex, wd: Double) => GBVertex(physicalId = v.physicalId,
        gbId = v.gbId,
        properties = v.properties + Property(arguments.outputPropertyName, wd))
    })

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)

    new AnnotateWeightedDegreesReturn(frameRddMap.keys.map(label => {
      val result = tryNew(CreateEntityArgs(description = Some("created by annotate weighted degrees operation"))) { newOutputFrame: FrameMeta =>
        val frameRdd = frameRddMap(label)
        save(new SparkFrameData(newOutputFrame.meta, frameRdd))
      }.meta
      (label, result)
    }).toMap)

  }
}
