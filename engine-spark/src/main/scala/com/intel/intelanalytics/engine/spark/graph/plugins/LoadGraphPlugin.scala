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

import com.intel.graphbuilder.driver.spark.titan.GraphBuilder
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.graph.{ GraphLoad, Graph }
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.SparkFrameStorage
import com.intel.intelanalytics.engine.spark.graph.GraphBuilderConfigFactory
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.rdd.RDD

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Loads graph data into a graph in the database. The source is tabular data interpreted by user-specified rules.
 */
class LoadGraphPlugin extends SparkCommandPlugin[GraphLoad, Graph] {

  /**
   * The name of the command, e.g. graph/sampling/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/load"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = None

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: GraphLoad)(implicit invocation: Invocation) = 2

  /**
   * Loads graph data into a graph in the database. The source is tabular data interpreted by user-specified rules.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: GraphLoad)(implicit invocation: Invocation): Graph = {
    // dependencies (later to be replaced with dependency injection)
    val graphs = engine.graphs
    val frames = engine.frames.asInstanceOf[SparkFrameStorage]

    // validate arguments
    arguments.frameRules.foreach(frule => frames.expectFrame(frule.frame))
    val frameRules = arguments.frameRules
    // TODO graphbuilder only supports one input frame at present
    require(frameRules.size == 1, "only one frame rule per call is supported in this version")
    val theOnlySourceFrameID = frameRules.head.frame.id
    val frameEntity = frames.expectFrame(theOnlySourceFrameID)
    val graphEntity = graphs.expectGraph(arguments.graph.id)

    // setup graph builder
    val gbConfigFactory = new GraphBuilderConfigFactory(frameEntity.schema, arguments, graphEntity)
    val graphBuilder = new GraphBuilder(gbConfigFactory.graphConfig)

    // setup data in Spark
    val inputRowsRdd: RDD[Rows.Row] = frames.loadLegacyFrameRdd(sc, theOnlySourceFrameID)
    val inputRdd: RDD[Seq[_]] = inputRowsRdd.map(x => x.toSeq)
    graphBuilder.build(inputRdd)

    graphEntity
  }

}
