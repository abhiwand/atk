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

package com.intel.intelanalytics.algorithm.graph

import com.intel.giraph.algorithms.cc.ConnectedComponentsComputation.{ ConnectedComponentsAggregatorWriter, ConnectedComponentsMasterCompute }
import com.intel.giraph.algorithms.pr.PageRankComputation
import com.intel.giraph.io.titan.formats.{ TitanVertexOutputFormatLongIDLongValue, TitanVertexInputFormatLongLongNull, TitanVertexInputFormatLongDoubleNull }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.algorithm.util.{ GiraphJobManager, GiraphConfigurationUtil }
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._
import com.intel.giraph.algorithms.cc.ConnectedComponentsComputation
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

case class ConnectedComponentsCommand(graph: GraphReference,
                                      @ArgDoc("""The name of edge label used to for performing the connected components
                                      calculation.""") inputEdgeLabel: String,
                                      @ArgDoc("""The vertex property which will contain the connected component id for
                                      each vertex.""") outputVertexProperty: String,
                                      @ArgDoc("""The convergence progress output interval.
                                      The valid value range is [1, max_supersteps].
                                      Default is 1 (output every superstep).""") convergenceProgressOutputInterval: Option[Int] = None)

case class ConnectedComponentsResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object ConnectedComponentsJsonFormat {
  import DomainJsonProtocol._
  implicit val connectedComponentsCommandFormat = jsonFormat4(ConnectedComponentsCommand)
  implicit val connectedComponentsResultFormat = jsonFormat1(ConnectedComponentsResult)
}

import ConnectedComponentsJsonFormat._
@PluginDoc(oneLine = "Make sub-graph of interconnected but isolated vertices.",
  extended = """Label vertices by their connected component in the graph induced by a given
edge label.

Notes
-----
It is prerequisite that the edge label in the property graph must be
bidirectional.""",
  returns = """The configuration and convergence report for Connected Components in the
format of a multiple-line string.""")
class ConnectedComponentsPlugin
    extends CommandPlugin[ConnectedComponentsCommand, ConnectedComponentsResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/connected_components"

  override def execute(arguments: ConnectedComponentsCommand)(implicit context: Invocation): ConnectedComponentsResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val graphFuture = engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "cc.convergenceProgressOutputInterval",
      arguments.convergenceProgressOutputInterval)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.inputEdgeLabel))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.outputVertexProperty))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatLongLongNull])
    giraphConf.
      setVertexOutputFormatClass(classOf[TitanVertexOutputFormatLongIDLongValue[_ <: org.apache.hadoop.io.LongWritable, _ <: org.apache.hadoop.io.LongWritable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[ConnectedComponentsMasterCompute])
    giraphConf.setComputationClass(classOf[ConnectedComponentsComputation])
    giraphConf.setAggregatorWriterClass(classOf[ConnectedComponentsAggregatorWriter])

    ConnectedComponentsResult(GiraphJobManager.run("ia_giraph_conncectedcomponents",
      classOf[ConnectedComponentsComputation].getCanonicalName,
      config, giraphConf, context, "cc-convergence-report_0"))
  }
}
