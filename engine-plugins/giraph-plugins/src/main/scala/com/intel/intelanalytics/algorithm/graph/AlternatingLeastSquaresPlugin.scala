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

import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.{ AlternatingLeastSquaresAggregatorWriter, AlternatingLeastSquaresMasterCompute }
import com.intel.giraph.io.titan.formats.{ TitanVertexOutputFormatPropertyGraph4CF, TitanVertexInputFormatPropertyGraph4CF }
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.algorithm.util.{ GiraphJobManager, GiraphConfigurationUtil }
import org.apache.giraph.conf.GiraphConfiguration
import scala.concurrent.duration._

import scala.concurrent._

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

case class AlternatingLeastSquares(graph: GraphReference,
                                   @ArgDoc("""The edge properties which contain the input edge values.
This is a single str.
If more than one edge property is used, this str is a comma-separated list of property names.""") edgeValuePropertyList: List[String],
                                   @ArgDoc("""Name of edge label.""") inputEdgeLabelList: List[String],
                                   @ArgDoc("""The list of vertex properties to store output vertex values.""") outputVertexPropertyList: List[String],
                                   @ArgDoc("""The name of vertex property which contains vertex type.
Vertices must have a property to identify them as either left-side (\"L\") or right-side (\"R\").""") vertexTypePropertyKey: String,
                                   @ArgDoc("""The name of edge property which contains edge type.""") edgeTypePropertyKey: String,
                                   @ArgDoc("""\"True\" means a vector as vertex value is supported,
\"False\" means a vector as vertex value is not supported.
Default is \"False\".""") vectorValue: Option[Boolean] = None,
                                   @ArgDoc("""The maximum number of supersteps (iterations) that the
algorithm will execute.
Default is 20.""") maxSupersteps: Option[Int] = None,
                                   @ArgDoc("""The amount of change in cost function that will be tolerated at
convergence.
If the change is less than this threshold, the algorithm exits earlier
before it reaches the maximum number of supersteps.
The valid value range is all float and zero.
Default is 0.""") convergenceThreshold: Option[Double] = None,
                                   @ArgDoc("""The tradeoff parameter that controls the strength of
regularization.
Larger value implies stronger regularization that helps prevent
overfitting but may cause the issue of underfitting if the value is
too large.
The value is usually determined by cross validation (CV).
The valid value range is all positive float and zero.
Default is 0.065.""") alsLambda: Option[Float] = None,
                                   @ArgDoc("""The length of feature vector to use in ALS model.
Larger value in general results in more accurate parameter estimation,
but slows down the computation.
The valid value range is all positive int.
Default is 3.""") featureDimension: Option[Int] = None,
                                   @ArgDoc("""The learning curve output interval.
Each ALS iteration is composed of 2 supersteps.
Default is 1 (2 supersteps).""") learningCurveOutputInterval: Option[Int] = None,
                                   @ArgDoc("""Checks if the graph meets certain structural requirements
before starting
the algorithm: at every vertex, the in-degree equals the out-degree.
ALS expects an undirected graph, so this is a necessary
but insufficient indication of validity.""") validateGraphStructure: Option[Boolean] = None,
                                   @ArgDoc("""True means turn on the update for bias term and False means turn off
the update for bias term.
Turning it on often yields more accurate model with minor performance
penalty; turning it off disables term update and leaves the value of
the bias term at zero.
Default is False.""") biasOn: Option[Boolean] = None,
                                   @ArgDoc("""The maximum edge weight value.
If an edge weight is larger than this
value, the algorithm will throw an exception and terminate.
This option is mainly for graph integrity check.
Valid value range is all float.
Default is Infinity.""") maxValue: Option[Float] = None,
                                   @ArgDoc("""The minimum edge weight value.
If an edge weight is smaller than this value,
the algorithm will throw an exception and terminate.
This option is mainly for graph integrity check.
Valid value range is all float.
Default is -Infinity.""") minValue: Option[Float] = None) {
}
case class AlternatingLeastSquaresResult(value: String)

/** Json conversion for arguments and return value case classes */
object AlternatingLeastSquaresJsonFormat {
  import DomainJsonProtocol._
  implicit val alsFormat = jsonFormat16(AlternatingLeastSquares)
  implicit val alsResultFormat = jsonFormat1(AlternatingLeastSquaresResult)
}

import AlternatingLeastSquaresJsonFormat._

@PluginDoc(oneLine = "Minimizing goodness-of-fit data measure in a series of steps.",
  extended = """The Alternating Least Squares with Bias for collaborative filtering algorithms.
The algorithms presented in:

1.  Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan.
    Large-Scale Parallel Collaborative Filtering for the Netflix Prize.
    2008.
2.  Y. Koren.
    Factorization Meets the Neighborhood: a Multifaceted Collaborative
    Filtering Model.
    In ACM KDD 2008. (Equation 5)

Notes
-----
Vertices must be identified as left-side (\"L\") or right-side (\"R\").""",
  returns = """The configuration and learning curve report for ALS in the format of a
multiple-line string.""")
class AlternatingLeastSquaresPlugin
    extends CommandPlugin[AlternatingLeastSquares, AlternatingLeastSquaresResult] {

  /**
   * The name of the command, e.g. graphs/ml/alternating_least_squares
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/ml/alternating_least_squares"

  override def execute(arguments: AlternatingLeastSquares)(implicit context: Invocation): AlternatingLeastSquaresResult = {

    val config = configuration
    val pattern = "[\\s,\\t]+"
    val outputVertexPropertyList = arguments.outputVertexPropertyList.mkString(",")
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
    GiraphConfigurationUtil.set(hConf, "als.maxSupersteps", arguments.maxSupersteps)
    GiraphConfigurationUtil.set(hConf, "als.convergenceThreshold", arguments.convergenceThreshold)
    GiraphConfigurationUtil.set(hConf, "als.featureDimension", arguments.featureDimension)
    GiraphConfigurationUtil.set(hConf, "als.bidirectionalCheck", arguments.validateGraphStructure)
    GiraphConfigurationUtil.set(hConf, "als.biasOn", arguments.biasOn)
    GiraphConfigurationUtil.set(hConf, "als.lambda", arguments.alsLambda)
    GiraphConfigurationUtil.set(hConf, "als.learningCurveOutputInterval", arguments.learningCurveOutputInterval)
    GiraphConfigurationUtil.set(hConf, "als.maxVal", arguments.maxValue)
    GiraphConfigurationUtil.set(hConf, "als.minVal", arguments.minValue)

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

    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatPropertyGraph4CF])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4CF[_ <: org.apache.hadoop.io.WritableComparable[_], _ <: org.apache.hadoop.io.Writable, _ <: org.apache.hadoop.io.Writable]])
    giraphConf.setMasterComputeClass(classOf[AlternatingLeastSquaresMasterCompute])
    giraphConf.setComputationClass(classOf[AlternatingLeastSquaresComputation])
    giraphConf.setAggregatorWriterClass(classOf[AlternatingLeastSquaresAggregatorWriter])

    AlternatingLeastSquaresResult(GiraphJobManager.run("ia_giraph_als",
      classOf[AlternatingLeastSquaresComputation].getCanonicalName,
      config, giraphConf, context, "als-learning-report_0"))
  }

}
