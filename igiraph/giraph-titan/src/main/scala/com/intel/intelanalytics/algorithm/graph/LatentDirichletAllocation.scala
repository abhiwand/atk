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

import com.intel.giraph.algorithms.lda.CVB0LDAComputation
import com.intel.giraph.io.VertexData4LDAWritable
import com.intel.giraph.io.titan.hbase.TitanHBaseVertexInputFormatPropertyGraph4LDA
import com.intel.giraph.io.titan.TitanVertexOutputFormatPropertyGraph4LDA
import com.intel.intelanalytics.domain.DomainJsonProtocol
import com.intel.intelanalytics.domain.graph.GraphReference
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.security.UserPrincipal
import com.intel.intelanalytics.algorithm.util.{ GiraphJobManager, GiraphConfigurationUtil }
import com.intel.mahout.math.DoubleWithVectorWritable
import org.apache.giraph.conf.GiraphConfiguration
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.concurrent.duration._

import scala.concurrent._
import scala.collection.JavaConverters._
import com.intel.intelanalytics.domain.command.CommandDoc

case class Lda(graph: GraphReference,
               edge_value_property_list: Option[String],
               input_edge_label_list: Option[String],
               output_vertex_property_list: Option[String],
               vertex_type_property_key: Option[String],
               vector_value: Option[String],
               max_supersteps: Option[Int] = None,
               alpha: Option[Float] = None,
               beta: Option[Float] = None,
               convergence_threshold: Option[Double] = None,
               evaluation_cost: Option[Boolean] = None,
               max_value: Option[Float] = None,
               min_value: Option[Float] = None,
               bidirectional_check: Option[Boolean] = None,
               num_topics: Option[Int] = None)

case class LdaResult(value: String)

class LatentDirichletAllocation
    extends CommandPlugin[Lda, LdaResult] {
  import DomainJsonProtocol._
  implicit val ldaFormat = jsonFormat15(Lda)
  implicit val ldaResultFormat = jsonFormat1(LdaResult)

  override def doc = Some(CommandDoc(oneLineSummary = "The `Latent Dirichlet Allocation <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`_",
    extendedSummary = Some("""
                           |    Parameters
                           |    ----------
                           |    edge_value_property_list : comma-separated string
                           |        The edge properties which contain the input edge values.
                           |        We expect comma-separated list of property names  if you use
                           |        more than one edge property.
                           | 
                           |    input_edge_label_list : comma-separated string
                           |        The name of edge label.
                           | 
                           |    output_vertex_property_list : Comma Separated List
                           |        The list of vertex properties to store output vertex values.
                           | 
                           |    vertex_type : string
                           |        The name of vertex property which contains vertex type.
                           | 
                           |    vector_value: Boolean
                           |        True means a vector as vertex value is supported
                           |        False means a vector as vertex value is not supported
                           | 
                           |    max_supersteps : integer (optional)
                           |        The maximum number of super steps (iterations) that the algorithm
                           |        will execute.
                           |        The valid value range is all positive integer.
                           |        The default value is 20.
                           | 
                           |    alpha : float (optional)
                           |        The hyper-parameter for document-specific distribution over topics.
                           |        It's mainly used as a smoothing parameter in Bayesian inference.
                           |        Larger value implies that documents are assumed to cover all topics
                           |        more uniformly; smaller value implies that documents are more concentrated
                           |        on a small subset of topics.
                           |        Valid value range is all positive float.
                           |        The default value is 0.1.
                           | 
                           |    beta : float (optional)
                           |        The hyper-parameter for word-specific distribution over topics.
                           |        It's mainly used as a smoothing parameter in Bayesian inference.
                           |        Larger value implies that topics contain all words more uniformly and
                           |        smaller value implies that topics are more concentrated on a small
                           |        subset of words.
                           |        Valid value range is all positive float.
                           |        The default value is 0.1.
                           | 
                           |    convergence_threshold : float (optional)
                           |        The amount of change in LDA model parameters that will be tolerated
                           |        at convergence. If the change is less than this threshold, the algorithm
                           |        exists earlier before it reaches the maximum number of super steps.
                           |        Valid value range is all positive float and zero.
                           |        The default value is 0.001.
                           | 
                           |    evaluate_cost : string (optional)
                           |        "True" means turn on cost evaluation and "False" means turn off
                           |        cost evaluation. It's relatively expensive for LDA to evaluate cost function.
                           |        For time-critical applications, this option allows user to turn off cost
                           |        function evaluation.
                           |        The default value is False.
                           | 
                           |    max_val : float (optional)
                           |        The maximum edge weight value. If an edge weight is larger than this
                           |        value, the algorithm will throw an exception and terminate. This option
                           |        is mainly for graph integrity check.
                           |        Valid value range is all float.
                           |        The default value is "Infinity".
                           | 
                           |    min_val : float (optional)
                           |        The minimum edge weight value. If an edge weight is smaller than this
                           |        value, the algorithm will throw an exception and terminate. This option
                           |        is mainly for graph integrity check.
                           |        Valid value range is all float.
                           |        The default value is "-Infinity".
                           | 
                           |    bidirectional_check : Boolean (optional)
                           |        True means to turn on bidirectional check. False means to turn
                           |        off bidirectional check. LDA expects a bi-partite input graph and
                           |        each edge therefore should be bi-directional. This option is mainly
                           |        for graph integrity check.
                           | 
                           |    num_topics : integer (optional)
                           |        The number of topics to identify in the LDA model. Using fewer
                           |        topics will speed up the computation, but the extracted topics
                           |        might be more abstract or less specific; using more topics will
                           |        result in more computation but lead to more specific topics.
                           |        Valid value range is all positive integers.
                           |        The default value is 10.
                           | 
                           |    Returns
                           |    -------
                           |    Multiple line string
                           |        The configuration and learning curve report for Latent Dirichlet Allocation.
                           | 
                           |    Examples
                           |    --------
                           |    ::
                           | 
                           |        g.ml.latent_dirichlet_allocation(edge_value_property_list = "word_count", vertex_type_property_key = "vertex_type", input_edge_label_list = "contains", output_vertex_property_list = "lda_result ", vector_value = "true", num_topics = 3)
                           | 
                           |    The expected output is like this::
                           | 
                           |        {u'value': u'======Graph Statistics======\\nNumber of vertices: 12 (doc: 6, word: 6)\\nNumber of edges: 12\\n\\n======LDA Configuration======\\nnumTopics: 3\\nalpha: 0.100000\\nbeta: 0.100000\\nconvergenceThreshold: 0.000000\\nbidirectionalCheck: false\\nmaxSupersteps: 20\\nmaxVal: Infinity\\nminVal: -Infinity\\nevaluateCost: false\\n\\n======Learning Progress======\\nsuperstep = 1\\tmaxDelta = 0.333682\\nsuperstep = 2\\tmaxDelta = 0.117571\\nsuperstep = 3\\tmaxDelta = 0.073708\\nsuperstep = 4\\tmaxDelta = 0.053260\\nsuperstep = 5\\tmaxDelta = 0.038495\\nsuperstep = 6\\tmaxDelta = 0.028494\\nsuperstep = 7\\tmaxDelta = 0.020819\\nsuperstep = 8\\tmaxDelta = 0.015374\\nsuperstep = 9\\tmaxDelta = 0.011267\\nsuperstep = 10\\tmaxDelta = 0.008305\\nsuperstep = 11\\tmaxDelta = 0.006096\\nsuperstep = 12\\tmaxDelta = 0.004488\\nsuperstep = 13\\tmaxDelta = 0.003297\\nsuperstep = 14\\tmaxDelta = 0.002426\\nsuperstep = 15\\tmaxDelta = 0.001783\\nsuperstep = 16\\tmaxDelta = 0.001311\\nsuperstep = 17\\tmaxDelta = 0.000964\\nsuperstep = 18\\tmaxDelta = 0.000709\\nsuperstep = 19\\tmaxDelta = 0.000521\\nsuperstep = 20\\tmaxDelta = 0.000383'}
                           | 
                            """.stripMargin)))

  override def execute(invocation: Invocation, arguments: Lda)(implicit user: UserPrincipal, executionContext: ExecutionContext): LdaResult = {

    val config = configuration
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val titanConf = GiraphConfigurationUtil.flattenConfig(config.getConfig("titan"), "titan.")

    val graphFuture = invocation.engine.getGraph(arguments.graph.id)
    val graph = Await.result(graphFuture, config.getInt("default-timeout") seconds)

    //    These parameters are set from the arguments passed in, or defaulted from
    //    the engine configuration if not passed.
    GiraphConfigurationUtil.set(hConf, "lda.maxSupersteps", arguments.max_supersteps)
    GiraphConfigurationUtil.set(hConf, "lda.alpha", arguments.alpha)
    GiraphConfigurationUtil.set(hConf, "lda.beta", arguments.beta)
    GiraphConfigurationUtil.set(hConf, "lda.convergenceThreshold", arguments.convergence_threshold)
    GiraphConfigurationUtil.set(hConf, "lda.evaluateCost", arguments.evaluation_cost)
    GiraphConfigurationUtil.set(hConf, "lda.maxVal", arguments.max_value)
    GiraphConfigurationUtil.set(hConf, "lda.minVal", arguments.min_value)
    GiraphConfigurationUtil.set(hConf, "lda.bidirectionalCheck", arguments.bidirectional_check)
    GiraphConfigurationUtil.set(hConf, "lda.numTopics", arguments.num_topics)

    GiraphConfigurationUtil.initializeTitanConfig(hConf, titanConf, graph)

    GiraphConfigurationUtil.set(hConf, "input.edge.value.property.key.list", arguments.edge_value_property_list)
    GiraphConfigurationUtil.set(hConf, "input.edge.label.list", arguments.input_edge_label_list)
    GiraphConfigurationUtil.set(hConf, "output.vertex.property.key.list", arguments.output_vertex_property_list)
    GiraphConfigurationUtil.set(hConf, "vertex.type.property.key", arguments.vertex_type_property_key)
    GiraphConfigurationUtil.set(hConf, "vector.value", arguments.vector_value)

    val giraphConf = new GiraphConfiguration(hConf)

    giraphConf.setVertexInputFormatClass(classOf[TitanHBaseVertexInputFormatPropertyGraph4LDA])
    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LDA[_ <: org.apache.hadoop.io.LongWritable, _ <: com.intel.giraph.io.VertexData4LPWritable, _ <: com.intel.mahout.math.DoubleWithVectorWritable]])
    giraphConf.setMasterComputeClass(classOf[CVB0LDAComputation.CVB0LDAMasterCompute])
    giraphConf.setComputationClass(classOf[CVB0LDAComputation])
    giraphConf.setAggregatorWriterClass(classOf[CVB0LDAComputation.CVB0LDAAggregatorWriter])

    LdaResult(GiraphJobManager.run("ia_giraph_lda",
      classOf[CVB0LDAComputation].getCanonicalName,
      config, giraphConf, invocation, "lda-learning-report_0"))
  }

  //TODO: Replace with generic code that works on any case class
  def parseArguments(arguments: JsObject) = arguments.convertTo[Lda]

  //TODO: Replace with generic code that works on any case class
  def serializeReturn(returnValue: LdaResult): JsObject = returnValue.toJson.asJsObject

  override def name: String = "graphs/ml/latent_dirichlet_allocation"

  //TODO: Replace with generic code that works on any case class
  override def serializeArguments(arguments: Lda): JsObject = arguments.toJson.asJsObject()
}
