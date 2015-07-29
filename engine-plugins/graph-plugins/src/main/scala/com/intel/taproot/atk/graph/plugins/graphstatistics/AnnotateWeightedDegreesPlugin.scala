/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.taproot.atk.graph.plugins.graphstatistics

import com.intel.taproot.analytics.engine.graph.SparkGraph
import com.intel.taproot.graphbuilder.elements.{ GBVertex, Property }
import com.intel.taproot.analytics.domain.frame.FrameEntity
import com.intel.taproot.analytics.domain.{ CreateEntityArgs, StorageFormats, DomainJsonProtocol }
import com.intel.taproot.analytics.domain.graph.{ GraphTemplate, GraphEntity, GraphReference }
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.{ SparkContextFactory, EngineConfig }
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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

    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

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
      val result = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by annotate weighted degrees operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = frameRddMap(label)
        newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap)

  }
}