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

import com.intel.giraph.algorithms.cgd.ConjugateGradientDescentComputation
import com.intel.giraph.algorithms.cgd.ConjugateGradientDescentComputation.{ ConjugateGradientDescentAggregatorWriter, ConjugateGradientDescentMasterCompute }
import com.intel.giraph.io.titan.formats.{ TitanVertexOutputFormatPropertyGraph4CF, TitanVertexInputFormatPropertyGraph4CFCGD, TitanVertexInputFormatPropertyGraph4CF }
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
import scala.collection.JavaConverters._
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

case class ConjugateGradientDescent(graph: GraphReference,
                                    @ArgDoc("""The edge properties which contain the input edge values.
A comma-separated list of property names when declaring
more than one edge property.""")
                                    edgeValuePropertyList: List[String],
                                    @ArgDoc("""The name of edge label.""")
                                    inputEdgeLabelList: List[String],
                                    @ArgDoc("""The list of vertex properties to store output vertex values.""")
                                    outputVertexPropertyList: List[String],
                                    @ArgDoc("""The name of vertex property which contains vertex type.
Vertices must have a property to identify them as either left-side
("L") or right-side ("R").""")
                                    vertexTypePropertyKey: String,
                                    @ArgDoc("""The name of edge property which contains edge type.""")
                                    edgeTypePropertyKey: String,
                                    @ArgDoc(""""True" means a vector as vertex value is supported,
"False" means a vector as vertex value is not supported.
Default is "False".""")
                                    vectorValue: Option[Boolean] = None,
                                    @ArgDoc("""The maximum number of supersteps (iterations) that the algorithm
will execute.
Default is 20.""")
                                    maxSupersteps: Option[Int] = None,
                                    @ArgDoc("""The amount of change in cost function that will be tolerated at
convergence.
If the change is less than this threshold, the algorithm exits
before it reaches the maximum number of supersteps.
The valid value range is all float and zero.
Default is 0.""")
                                    convergenceThreshold: Option[Double] = None,
                                    @ArgDoc("""The tradeoff parameter that controls the strength of regularization.
Larger value implies stronger regularization that helps prevent
overfitting but may cause the issue of underfitting if the value is too
large.
The value is usually determined by cross validation (CV).
The valid value range is all positive float and zero.
Default is 0.065.""")
                                    cgdLambda: Option[Float] = None,
                                    @ArgDoc("""The length of feature vector to use in CGD model.
Larger value in general results in more accurate parameter estimation,
but slows down the computation.
The valid value range is all positive int.
Default is 3.""")
                                    featureDimension: Option[Int] = None,
                                    @ArgDoc("""The learning curve output interval.
Each CGD iteration is composed of 2 supersteps.
Default is 1 (means two supersteps).""")
                                    learningCurveOutputInterval: Option[Int] = None,
                                    @ArgDoc("""Checks if the graph meets certain structural requirements before starting
the algorithm: at every vertex, the in-degree equals the out-degree.
This algorithm is intended for undirected graphs.
Therefore, this is a necessary, but insufficient, check for valid input.""")
                                    validateGraphStructure: Option[Boolean] = None,
                                    @ArgDoc("""True means turn on the update for bias term and False means turn off
the update for bias term.
Turning it on often yields more accurate model with minor performance
penalty.
Turning it off disables term update and treats the value of
bias term as 0.
Default is False.""")
                                    biasOn: Option[Boolean] = None,
                                    @ArgDoc("""The maximum edge weight value.
If an edge weight is larger than this value, the algorithm will throw an
exception and terminate.
This option is mainly for graph integrity check.
Valid value range is all float.
Default is Infinity.""")
                                    maxValue: Option[Float] = None,
                                    @ArgDoc("""The minimum edge weight value.
If an edge weight is smaller than this value, the algorithm will throw an
exception and terminate.
This option is mainly for graph integrity check.
Valid value range is all float.
Default is -Infinity.""")
                                    minValue: Option[Float] = None,
                                    @ArgDoc("")
                                    numIters: Option[Int] = None) {
}

case class ConjugateGradientDescentResult(value: String)

/** Json conversion for arguments and return value case classes */
object ConjugateGradientDescentJsonFormat {
  import DomainJsonProtocol._
  implicit val cgdFormat = jsonFormat17(ConjugateGradientDescent)
  implicit val cgdResultFormat = jsonFormat1(ConjugateGradientDescentResult)
}

import ConjugateGradientDescentJsonFormat._
@PluginDoc(oneLine = "Personalized suggestions relyng on Collaborating Filtering.",
  extended = """The Conjugate Gradient Descent (CGD) with Bias for collaborative filtering
algorithms.

CGD implementation of the algorithm presented in Y. Koren.
Factorization Meets the Neighborhood: a Multifaceted Collaborative Filtering
Model.
In ACM KDD 2008. (Equation 5)

Notes
-----
Vertices must be identified as left-side ("L") or right-side ("R").""",
  returns = """The configuration and learning curve report for CGD in the format of a
multiple-line string.""")
class ConjugateGradientDescentPlugin
    extends CommandPlugin[ConjugateGradientDescent, ConjugateGradientDescentResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/ml/conjugate_gradient_descent"

  override def execute(arguments: ConjugateGradientDescent)(implicit context: Invocation): ConjugateGradientDescentResult = {

    val config = configuration
    val pattern = "[\\s,\\t]+"
    val outputVertexPropertyList = arguments.outputVertexPropertyList.mkString(argSeparator)
    val resultPropertyList = outputVertexPropertyList.split(pattern)
    val vectorValue = arguments.vectorValue.getOrElse(false)
    val biasOn = arguments.biasOn.getOrElse(false)
    require(resultPropertyList.size >= 1,
      "Please input at least one vertex property name for ALS/CGD results")
    require(!vectorValue || !biasOn ||
      (vectorValue && biasOn && resultPropertyList.size == 2),
      "Please input one property name for bias and one property name for results when both vector_value " +
        "and bias_on are enabled")
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val graphFuture = engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)
    val biasOnOption = if (biasOn) Option(biasOn.toString().toLowerCase()) else None

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "cgd.maxSupersteps", arguments.maxSupersteps)
    GiraphConfigurationUtil.set(hConf, "cgd.convergenceThreshold", arguments.convergenceThreshold)
    GiraphConfigurationUtil.set(hConf, "cgd.featureDimension", arguments.featureDimension)
    GiraphConfigurationUtil.set(hConf, "cgd.bidirectionalCheck", arguments.validateGraphStructure)
    GiraphConfigurationUtil.set(hConf, "cgd.biasOn", arguments.biasOn)
    GiraphConfigurationUtil.set(hConf, "cgd.lambda", arguments.cgdLambda)
    GiraphConfigurationUtil.set(hConf, "cgd.learningCurveOutputInterval", arguments.learningCurveOutputInterval)
    GiraphConfigurationUtil.set(hConf, "cgd.maxVal", arguments.maxValue)
    GiraphConfigurationUtil.set(hConf, "cgd.minVal", arguments.minValue)
    GiraphConfigurationUtil.set(hConf, "cgd.numCGDIters", arguments.numIters)

    GiraphConfigurationUtil.set(hConf, "giraphjob.maxSteps", arguments.maxSupersteps)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, config, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", Some(arguments.edgeValuePropertyList.mkString(argSeparator)))
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", Some(arguments.inputEdgeLabelList.mkString(argSeparator)))
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", Some(arguments.outputVertexPropertyList.mkString(argSeparator)))
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", Some(arguments.vertexTypePropertyKey))
    GiraphConfigurationUtil.set(hConf, "edge.type.property.key", Some(arguments.edgeTypePropertyKey))
    GiraphConfigurationUtil.set(hConf, "vector.value", Some(vectorValue.toString))
    GiraphConfigurationUtil.set(hConf, "output.vertex.bias", Some(biasOn))

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatPropertyGraph4CFCGD])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4CF[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[ConjugateGradientDescentMasterCompute])
    giraphConf.setComputationClass(classOf[ConjugateGradientDescentComputation])
    giraphConf.setAggregatorWriterClass(classOf[ConjugateGradientDescentAggregatorWriter])

    ConjugateGradientDescentResult(GiraphJobManager.run("ia_giraph_cgd",
      classOf[ConjugateGradientDescentComputation].getCanonicalName,
      config, giraphConf, context, "cgd-learning-report_0"))
  }

}
