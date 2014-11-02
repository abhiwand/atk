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

package com.intel.intelanalytics.engine.spark.graph.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.graph.GraphNoArgs
import com.intel.intelanalytics.domain.LongValue
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.graph.SparkGraphStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Edge Count for a "seamless graph"
 */
class EdgeCountPlugin extends SparkCommandPlugin[GraphNoArgs, LongValue] {

  /**
   * The name of the command, e.g. graph:/edge_count
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:/edge_count"

  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Get the total number of edges in the graph.",
    extendedSummary = Some("""
    Get the total number of edges in the graph.

    Examples
    --------
                             |graph.edge_count()
                           """)))

  /**
   * Edge count for a "seamless graph"
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments passed by the user
   * @return the count
   */
  override def execute(arguments: GraphNoArgs)(implicit invocation: Invocation): LongValue = {
    val graphs = invocation.engine.graphs.asInstanceOf[SparkGraphStorage]
    val graph = graphs.expectSeamless(arguments.graph.id)
    LongValue(graph.edgeCount.get)
  }

}
