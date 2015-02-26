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

package com.intel.spark.graphon.beliefpropagation

import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.storage.StorageLevel
import scala.concurrent.{ Await, ExecutionContext }
import com.intel.intelanalytics.component.Boot
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.{ SparkGraphHBaseBackend, GraphBuilderConfigFactory }
import spray.json._
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ GBVertex, GBEdge }
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.parser.InputSchema
import com.intel.graphbuilder.driver.spark.rdd.GraphBuilderRDDImplicits._
import com.intel.intelanalytics.domain.command.CommandDoc
import org.apache.spark.{ SparkConf, SparkContext }
import com.intel.graphbuilder.util.SerializableBaseConfiguration

/**
 * Parameters for executing belief propagation.
 * @param graph Reference to the graph object on which to propagate beliefs.
 * @param priorProperty Name of the property that stores the prior beliefs.
 * @param posteriorProperty Name of the property to which posterior beliefs will be stored.
 * @param edgeWeightProperty Optional String. Name of the property on edges that stores the edge weight.
 *                           If none is supplied, edge weights default to 1.0
 * @param convergenceThreshold Optional Double. BP will terminate when average change in posterior beliefs between
 *                             supersteps is less than or equal to this threshold. Defaults to 0.
 * @param maxIterations Optional integer. The maximum number of iterations of message passing that will be invoked.
 *                      Defaults to 20.
 */
case class BeliefPropagationArgs(graph: GraphReference,
                                 priorProperty: String,
                                 posteriorProperty: String,
                                 edgeWeightProperty: Option[String] = None,
                                 convergenceThreshold: Option[Double] = None,
                                 maxIterations: Option[Int] = None)

/**
 * Companion object holds the default values.
 */
object BeliefPropagationDefaults {
  val stringOutputDefault = false
  val maxIterationsDefault = 20
  val edgeWeightDefault = 1.0d
  val powerDefault = 0d
  val smoothingDefault = 2.0d
  val convergenceThreshold = 0d
}

/**
 * The result object
 *
 * @param log execution log
 * @param time execution time
 */
case class BeliefPropagationResult(log: String, time: Double)

/** Json conversion for arguments and return value case classes */
object BeliefPropagationJsonFormat {
  import DomainJsonProtocol._
  implicit val BPFormat = jsonFormat6(BeliefPropagationArgs)
  implicit val BPResultFormat = jsonFormat2(BeliefPropagationResult)
}

import BeliefPropagationJsonFormat._

/**
 * Launches "loopy" belief propagation.
 *
 * Pulls graph from underlying store, sends it off to the LBP runner, and then sends results back to the underlying
 * store.
 *
 * Right now it is using only Titan for graph storage. In time we will hopefully make this more flexible.
 *
 */
class BeliefPropagation extends SparkCommandPlugin[BeliefPropagationArgs, BeliefPropagationResult] {

  override def name: String = "graph:titan/ml/belief_propagation"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: BeliefPropagationArgs)(implicit invocation: Invocation): Int = {
    // TODO: not sure this is right but it seemed to work with testing
    //    when max iterations was 1, number of jobs was 11
    //    when max iterations was 2, number of jobs was 13
    //    when max iterations was 3, number of jobs was 15
    //    when max iterations was 4, number of jobs was 17
    //    when max iterations was 5, number of jobs was 19
    //    when max iterations was 6, number of jobs was 21
    9 + (arguments.maxIterations.getOrElse(0) * 2)
  }

  override def execute(arguments: BeliefPropagationArgs)(implicit invocation: Invocation): BeliefPropagationResult = {

    val start = System.currentTimeMillis()
    val sparkContext = sc
    sparkContext.addJar(SparkContextFactory.jarPath("graphon"))

    // TODO: stopping the old spark context and restarting it here avoids a class not found error...
    // there has got to be a better way
    // This issue appears to be due to a classloader bug in Spark https://github.com/tribbloid/ISpark/issues/7
    // It will be fixed in Spark 1.1.1+
    sc.stop

    val ctx = new SparkContext(sc.getConf)

    try {
      ctx.addJar(SparkContextFactory.jarPath("graphon"))

      // Get the graph
      val graph = engine.graphs.expectGraph(arguments.graph.id)
      val (gbVertices, gbEdges) = engine.graphs.loadGbElements(ctx, graph)

      val bpRunnerArgs = BeliefPropagationRunnerArgs(arguments.posteriorProperty,
        arguments.priorProperty,
        arguments.maxIterations,
        stringOutput = Some(true), // string output is default until the ATK supports Vectors as a datatype in tables
        arguments.convergenceThreshold,
        arguments.edgeWeightProperty)

      val (outVertices, outEdges, log) = BeliefPropagationRunner.run(gbVertices, gbEdges, bpRunnerArgs)

      // edges do not change during this computation so we avoid the very expensive step of appending them into Titan

      val dummyOutEdges: RDD[GBEdge] = ctx.parallelize(List.empty[GBEdge])

      // write out the graph

      // Create the GraphBuilder object
      // Setting true to append for updating existing graph
      val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)
      val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig, append = true))
      // Build the graph using spark
      gb.buildGraphWithSpark(outVertices, dummyOutEdges)

      // Get the execution time and print it
      val time = (System.currentTimeMillis() - start).toDouble / 1000.0
      BeliefPropagationResult(log, time)
    }
    finally {
      ctx.stop
    }
  }
}
