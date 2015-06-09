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

package com.intel.intelanalytics.algorithm.model

import com.intel.giraph.algorithms.lda.CVB0LDAComputation
import com.intel.giraph.algorithms.lda.CVB0LDAComputation.{ CVB0LDAAggregatorWriter, CVB0LDAMasterCompute }
import com.intel.ia.giraph.lda.v2._
import com.intel.intelanalytics.algorithm.util.{ GiraphConfigurationUtil, GiraphJobManager }
import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.schema.{ DataTypes, Column, FrameSchema }
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, CommandInvocation, CommandPlugin, Invocation }
import org.apache.spark.sql.parquet.ia.giraph.frame.{ LdaParquetFrameEdgeInputFormat, LdaParquetFrameVertexOutputFormat }
import spray.json._
import LdaJsonFormat._
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
/**
 * Represents the arguments for Latent Dirichlet allocation
 *
 * @param graph Reference to the graph for which communities has to be determined.
 *
 * case class LdaTrainArgs(graph: GraphReference,
 *                         @ArgDoc("""Input frame data.""")
 *                         frame: Frame
 *                         @ArgDoc("""Column Name for documents.
 * Column should contain a str value.""")
 *                         doc: str
 *                         @ArgDoc("""Column name for words.
 * Column should contain a str value.""")
 *                         word: str
 *                         @ArgDoc("""Column name for word count.
 * Column should contain an int64 value.""")
 *                         word_count: str
 *                         @ArgDoc("""The maximum number of iterations that the algorithm will execute.
 * The valid value range is all positive int.
 * Default is 20.""")
 *                         max_interations: int (optional)
 *                         @ArgDoc("""The hyper-parameter for document-specific distribution over topics.
 * Mainly used as a smoothing parameter in :term:`Bayesian inference`.
 * Larger value implies that documents are assumed to cover all topics
 * more uniformly; smaller value implies that documents are more
 * concentrated on a small subset of topics.
 * Valid value range is all positive float.
 * Default is 0.1.""")
 *                         alpha: float (optional)
 *                         @ArgDoc("""The hyper-parameter for word-specific distribution over topics.
 * Mainly used as a smoothing parameter in :term:`Bayesian inference`.
 * Larger value implies that topics contain all words more uniformly and
 * smaller value implies that topics are more concentrated on a small
 * subset of words.
 * Valid value range is all positive float.
 * Default is 0.1.""")
 *                         beta: float (optional)
 *                         @ArgDoc("""The amount of change in LDA model parameters that will be tolerated
 * at convergence.
 * If the change is less than this threshold, the algorithm exits
 * before it reaches the maximum number of supersteps.
 * Valid value range is all positive float and 0.0.
 * Default is 0.001.""")
 *                         convergence_threshold: float (optional)
 *                         @ArgDoc(""""True" means turn on cost evaluation and "False" means turn off
 * cost evaluation.
 * It's relatively expensive for LDA to evaluate cost function.
 * For time-critical applications, this option allows user to turn off cost
 * function evaluation.
 * Default is "False".""")
 *                         evaluate_cost: bool (optional)
 *                         @ArgDoc("""The number of topics to identify in the LDA model.
 * Using fewer topics will speed up the computation, but the extracted topics
 * might be more abstract or less specific; using more topics will
 * result in more computation but lead to more specific topics.
 * Valid value range is all positive int.
 * Default is 10.""")
 *                         num_topics: int (optional)
 *
 */

/**
 * Latent Dirichlet allocation
 */
@PluginDoc(oneLine = "Creates Latent Dirichlet Allocation model.",
  extended = """The `Latent Dirichlet Allocation <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`""",
  returns = """dict
    The data returned is composed of multiple components:
doc_results : Frame
    Frame with LDA results.
word_results : Frame
    Frame with LDA results.
report : str
   The configuration and learning curve report for Latent Dirichlet
   Allocation as a multiple line str.""")
class LdaTrainPlugin
    extends CommandPlugin[LdaTrainArgs, LdaTrainResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lda/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  override def execute(arguments: LdaTrainArgs)(implicit invocation: Invocation): LdaTrainResult = {

    val frames = engine.frames
    val config = configuration

    // validate arguments
    val frame = frames.expectFrame(arguments.frame)
    frame.schema.requireColumnIsType(arguments.documentColumnName, DataTypes.string)
    frame.schema.requireColumnIsType(arguments.wordColumnName, DataTypes.string)
    frame.schema.requireColumnIsType(arguments.wordCountColumnName, DataTypes.int64)
    require(frame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    // setup and run
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")

    val giraphConf = new LdaConfiguration(hConf)

    val docOut = frames.prepareForSave(CreateEntityArgs(description = Some("LDA doc results")))
    val wordOut = frames.prepareForSave(CreateEntityArgs(description = Some("LDA word results")))

    val inputFormatConfig = new LdaInputFormatConfig(frame.storageLocation.get, frame.schema)
    val outputFormatConfig = new LdaOutputFormatConfig(docOut.storageLocation.get, wordOut.storageLocation.get)
    val ldaConfig = new LdaConfig(inputFormatConfig, outputFormatConfig, arguments)

    giraphConf.setLdaConfig(ldaConfig)
    GiraphConfigurationUtil.set(giraphConf, "giraphjob.maxSteps", arguments.maxIterations)

    //giraphConf.setVertexInputFormatClass(classOf[LdaParquetFrameVertexValueInputFormat])
    giraphConf.setEdgeInputFormatClass(classOf[LdaParquetFrameEdgeInputFormat])
    giraphConf.setVertexOutputFormatClass(classOf[LdaParquetFrameVertexOutputFormat])
    giraphConf.setMasterComputeClass(classOf[CVB0LDAMasterCompute])
    giraphConf.setComputationClass(classOf[CVB0LDAComputation])
    giraphConf.setAggregatorWriterClass(classOf[CVB0LDAAggregatorWriter])

    val report = GiraphJobManager.run(s"ia_giraph_lda_train_${invocation.asInstanceOf[CommandInvocation].commandId}",
      classOf[CVB0LDAComputation].getCanonicalName,
      config, giraphConf, invocation, "lda-learning-report_0")

    val resultsColumn = Column("lda_results", DataTypes.vector(arguments.getNumTopics))

    // After saving update timestamps, status, row count, etc.
    frames.postSave(None, docOut.toReference, new FrameSchema(List(frame.schema.column(arguments.documentColumnName), resultsColumn)))
    frames.postSave(None, wordOut.toReference, new FrameSchema(List(frame.schema.column(arguments.wordColumnName), resultsColumn)))

    LdaTrainResult(frames.expectFrame(docOut.toReference), frames.expectFrame(wordOut.toReference), report)
  }

}
