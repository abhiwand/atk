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

package com.intel.spark.graphon.beliefpropagation

import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.context.SparkContextFactory
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.graph.{ GraphBuilderConfigFactory }
import spray.json._
import org.apache.spark.rdd.RDD
import com.intel.graphbuilder.elements.{ GBEdge }
import com.intel.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import com.intel.graphbuilder.parser.InputSchema
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

/**
 * Parameters for executing belief propagation.
 * @param graph Reference to the graph object on which to propagate beliefs.
 */
case class BeliefPropagationArgs(graph: GraphReference,
                                 @ArgDoc("""Name of the vertex property which contains the prior belief
for the vertex.""") priorProperty: String,
                                 @ArgDoc("""Name of the vertex property which will contain the posterior
 belief for each vertex.""") posteriorProperty: String,
                                 @ArgDoc("""Name of the edge property that contains the edge weight for each edge.""") edgeWeightProperty: Option[String] = None,
                                 @ArgDoc("""Minimum average change in posterior beliefs between supersteps.
Belief propagation will terminate when the average change in posterior beliefs between supersteps is
less than or equal to this threshold.""") convergenceThreshold: Option[Double] = None,
                                 @ArgDoc("""The maximum number of supersteps that the algorithm will execute.
The valid range is all positive int.""") maxIterations: Option[Int] = None)

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
@PluginDoc(oneLine = "Classification on sparse data using belief propagation.",
  extended = """Belief propagation by the sum-product algorithm.
This algorithm analyzes a graphical model with prior beliefs using sum product message passing.
The priors are read from a property in the graph, the posteriors are written to another property in the graph.
This is the GraphX-based implementation of belief propagation.""",
  returns = "Progress report for belief propagation in the format of a multiple-line string.")
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

    if (!SparkEngineConfig.isSparkOnYarn)
      sc.addJar(SparkContextFactory.jarPath("graph-plugins"))

    // Get the graph
    val graph = engine.graphs.expectGraph(arguments.graph)
    val (gbVertices, gbEdges) = engine.graphs.loadGbElements(sc, graph)

    val bpRunnerArgs = BeliefPropagationRunnerArgs(arguments.posteriorProperty,
      arguments.priorProperty,
      arguments.maxIterations,
      stringOutput = Some(true), // string output is default until the ATK supports Vectors as a datatype in tables
      arguments.convergenceThreshold,
      arguments.edgeWeightProperty)

    val (outVertices, outEdges, log) = BeliefPropagationRunner.run(gbVertices, gbEdges, bpRunnerArgs)

    // edges do not change during this computation so we avoid the very expensive step of appending them into Titan

    val dummyOutEdges: RDD[GBEdge] = sc.parallelize(List.empty[GBEdge])

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
}
