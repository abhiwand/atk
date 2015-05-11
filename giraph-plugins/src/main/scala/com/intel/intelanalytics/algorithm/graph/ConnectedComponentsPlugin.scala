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

case class ConnectedComponentsCommand(graph: GraphReference,
                                      inputEdgeLabel: String,
                                      outputVertexProperty: String,
                                      convergenceProgressOutputInterval: Option[Int] = None)

case class ConnectedComponentsResult(value: String) //TODO

/** Json conversion for arguments and return value case classes */
object ConnectedComponentsJsonFormat {
  import DomainJsonProtocol._
  implicit val connectedComponentsCommandFormat = jsonFormat4(ConnectedComponentsCommand)
  implicit val connectedComponentsResultFormat = jsonFormat1(ConnectedComponentsResult)
}

import ConnectedComponentsJsonFormat._

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
