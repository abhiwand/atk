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

package com.intel.taproot.spark.graphon.graphstatistics

import com.intel.taproot.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.taproot.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import com.intel.taproot.graphbuilder.parser.InputSchema
import com.intel.taproot.graphbuilder.util.SerializableBaseConfiguration
import com.intel.taproot.analytics.domain.frame.{ FrameEntity }
import com.intel.taproot.analytics.domain.{ CreateEntityArgs, StorageFormats, DomainJsonProtocol }
import com.intel.taproot.analytics.domain.graph.{ GraphEntity, GraphTemplate, GraphReference }
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.spark.{ SparkContextFactory, EngineConfig }
import com.intel.taproot.analytics.engine.spark.frame.SparkFrameData
import com.intel.taproot.analytics.engine.spark.graph.GraphBuilderConfigFactory
import com.intel.taproot.analytics.engine.spark.graph._
import com.intel.taproot.analytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.UUID
import com.intel.taproot.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._

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
      val result = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by annotated degrees operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = frameRddMap(label)
        newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap)

  }
}
