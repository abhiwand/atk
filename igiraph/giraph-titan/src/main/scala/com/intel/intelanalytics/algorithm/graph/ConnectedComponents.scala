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

package com.intel.intelanalytics.algorithm.graph

import com.intel.giraph.algorithms.pr.PageRankComputation
import com.intel.giraph.io.titan.TitanVertexOutputFormatLongIDLongValue
import com.intel.giraph.io.titan.hbase.{ TitanHBaseVertexInputFormatLongLongNull, TitanHBaseVertexInputFormatLongDoubleNull }
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

case class ConnectedComponentsCommand(graph: GraphReference,
                                      input_edge_label: String,
                                      output_vertex_property: String,
                                      convergence_progress_output_interval: Option[Int] = None)

case class ConnectedComponentsResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object ConnectedComponentsJsonFormat {
  import DomainJsonProtocol._
  implicit val connectedComponentsCommandFormat = jsonFormat4(ConnectedComponentsCommand)
  implicit val connectedComponentsResultFormat = jsonFormat1(ConnectedComponentsResult)
}

import ConnectedComponentsJsonFormat._

class ConnectedComponents
    extends CommandPlugin[ConnectedComponentsCommand, ConnectedComponentsResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graphs/ml/connected_components"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc = Some(CommandDoc(oneLineSummary = "Label vertices by their connected component in the graph induced by a given edge label",
    extendedSummary = Some("""
                            |   Prerequisites::
                            |
                            |       Edge label in the property graph must be bidirectional.
                            |
                            |   Parameters
                            |   ----------
                            |   input_edge_label : string
                            |       The name of edge label used to for performing the connected components calculation.
                            |
                            |   output_vertex_property : string
                            |       The vertex property which will contain the connected component id for each vertex.
                            |
                            |   convergence_output_interval : integer (optional)
                            |       The convergence progress output interval.
                            |       Since convergence is a tricky notion for
                            |       The valid value range is [1, max_supersteps]
                            |       The default value is 1, which means output every super step.
                            |
                            |   Returns
                            |   -------
                            |   Multiple line string
                            |       The configuration and convergence report for Connected Components.
                            |
                            |   Examples
                            |   --------
                            |   ::
                            |
                            |       g.ml.conncted_components(input_edge_label = "edge", output_vertex_property = "component_id")
                            |
                            """.stripMargin)))

  override def execute(invocation: Invocation, arguments: ConnectedComponentsCommand)(implicit user: UserPrincipal, executionContext: ExecutionContext): ConnectedComponentsResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val titanConf = GiraphConfigurationUtil.flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "cc.convergenceProgressOutputInterval",
      arguments.convergence_progress_output_interval)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, titanConf, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.input_edge_label))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.output_vertex_property))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatLongLongNull])
    giraphConf.
      setVertexOutputFormatClass(classOf[TitanVertexOutputFormatLongIDLongValue[_ <: org.apache.hadoop.io.LongWritable, _ <: org.apache.hadoop.io.LongWritable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[ConnectedComponentsComputation.ConnectedComponentsMasterCompute])
    giraphConf.setComputationClass(classOf[ConnectedComponentsComputation])
    giraphConf.setAggregatorWriterClass(classOf[ConnectedComponentsComputation.ConnectedComponentsAggregatorWriter])

    ConnectedComponentsResult(GiraphJobManager.run("ia_giraph_conncectedcomponents",
      classOf[ConnectedComponentsComputation].getCanonicalName,
      config, giraphConf, invocation, "cc-convergence-report_0"))
  }
}
