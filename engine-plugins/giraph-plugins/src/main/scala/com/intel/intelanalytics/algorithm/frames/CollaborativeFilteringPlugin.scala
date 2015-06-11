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

package com.intel.intelanalytics.algorithm.frames

import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation
import com.intel.giraph.algorithms.als.AlternatingLeastSquaresComputation.{ AlternatingLeastSquaresAggregatorWriter, AlternatingLeastSquaresMasterCompute }
import com.intel.giraph.algorithms.cgd.ConjugateGradientDescentComputation
import com.intel.giraph.algorithms.cgd.ConjugateGradientDescentComputation.{ ConjugateGradientDescentAggregatorWriter, ConjugateGradientDescentMasterCompute }
import com.intel.giraph.io.{ VertexData4CGDWritable, VertexData4CFWritable }
import com.intel.ia.giraph.cf._
import com.intel.intelanalytics.algorithm.util.{ GiraphConfigurationUtil, GiraphJobManager }
import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.schema.{ DataTypes, Column, FrameSchema }
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import org.apache.spark.sql.parquet.ia.giraph.frame.cf.{ CollaborativeFilteringVertexOutputFormat, CollaborativeFilteringEdgeInputFormat }
import CollaborativeFilteringJsonFormat._

/*
Parameters
----------
user_col_name: str
    The column name for the user vertex id.
    The valid values are numeric strings (such as "1", "5003")
item_col_name: str
    The column name for the item vertex id.
        The valid values are strings (such as "A", "B")
rating_col_name: str
    The column name for the edge rating.
    The valid values are numeric (such as 1, 3,5)
evaluation_function: str (optional)
    "als" or "cgd" as evaluation functions.
    The default value is "als"
num_factors : str (optional)
    number of entries in the (output) factors vector
        The default value is 3
max_iterations : int (optional)
    The maximum number of supersteps that the algorithm will execute.
    The valid value range is all positive int.
    The default value is 10.
convergence_threshold : float (optional)
    The amount of change in cost function that will be tolerated at
    convergence.
    If the change is less than this threshold, the algorithm exits earlier
    before it reaches the maximum number of supersteps.
    The valid value range is all float and zero.
    The default value is 0.00000001f.
lambda : float (optional)
    The tradeoff parameter that controls how much influence an external
    classifier's prediction contributes to the final prediction.
    This is for the case where an external classifier is available that can
    produce initial probabilistic classification on unlabeled examples, and
    the option allows incorporating external classifier's prediction into
    the LP training process.
    The valid value range is [0.0,1.0].
    The default value is 0.
bias_on : boolean (optional)
    Bias on/off switch
    The default value is "true"
max_value : int (optional)
    Maximum edge rating value.
    The default value is 10.
min_value : int (optional)
    Maximum edge rating value.
    The default value is 0.
learning_curve_interval : int (optional)
    Iteration interval to output learning curve.
    The default value is 1.
cgd_iterations : int (optional)
    Number of CGD iterations in each super step
    The default value is 2.
*/

@PluginDoc(oneLine = "Minimizing goodness-of-fit data measure in a series of steps.",
  extended = """The ALS/CGD with Bias for collaborative filtering algorithms.
The algorithms presented in:

1.  Y. Zhou, D. Wilkinson, R. Schreiber and R. Pan.
    Large-Scale Parallel Collaborative Filtering for the Netflix Prize.
    2008.
2.  Y. Koren.
    Factorization Meets the Neighborhood: a Multifaceted Collaborative
    Filtering Model.
    In ACM KDD 2008. (Equation 5)""",
  returns = """two 2-column frames and a report:

user vertex: int
    A vertex id.
result : Vector (long)
    feature vector for the vertex

item vertex: int
    A vertex id.
result : Vector (long)
    feature vector for the item (same as the vector for the corresponding user)

report: string""")
class CollaborativeFilteringPlugin
    extends CommandPlugin[CollaborativeFilteringArgs, CollaborativeFilteringResult] {

  /**
   * The name of the command, e.g. frame:/label_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/collaborative_filtering"

  override def execute(arguments: CollaborativeFilteringArgs)(implicit context: Invocation): CollaborativeFilteringResult = {

    val frames = engine.frames
    val config = configuration
    val frame = frames.expectFrame(arguments.frame)
    require(frame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    // setup and run
    val hadoopConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val giraphConf = new CollaborativeFilteringConfiguration(hadoopConf)

    val userFrame = frames.prepareForSave(CreateEntityArgs(description = Some("Collaborative filtering user frame results")))
    val itemFrame = frames.prepareForSave(CreateEntityArgs(description = Some("Collaborative filtering item frame results")))
    val inputFormatConfig = new CollaborativeFilteringInputFormatConfig(frame.storageLocation.get, frame.schema)
    val outputFormatConfig = new CollaborativeFilteringOutputFormatConfig(userFrame.storageLocation.get, itemFrame.storageLocation.get)
    val collaborativeFilteringConfig = new CollaborativeFilteringConfig(inputFormatConfig, outputFormatConfig, arguments)

    giraphConf.setConfig(collaborativeFilteringConfig)
    GiraphConfigurationUtil.set(giraphConf, "giraphjob.maxSteps", arguments.maxIterations)

    giraphConf.setEdgeInputFormatClass(classOf[CollaborativeFilteringEdgeInputFormat])

    var computation: String = null
    if (arguments.alsAlgorithm.equalsIgnoreCase(collaborativeFilteringConfig.evaluationFunction)) {
      giraphConf.setVertexOutputFormatClass(classOf[CollaborativeFilteringVertexOutputFormat[VertexData4CFWritable]])
      giraphConf.setMasterComputeClass(classOf[AlternatingLeastSquaresMasterCompute])
      giraphConf.setComputationClass(classOf[AlternatingLeastSquaresComputation])
      giraphConf.setAggregatorWriterClass(classOf[AlternatingLeastSquaresAggregatorWriter])
      computation = classOf[AlternatingLeastSquaresComputation].getCanonicalName
    }
    else {
      giraphConf.setVertexOutputFormatClass(classOf[CollaborativeFilteringVertexOutputFormat[VertexData4CGDWritable]])
      giraphConf.setMasterComputeClass(classOf[ConjugateGradientDescentMasterCompute])
      giraphConf.setComputationClass(classOf[ConjugateGradientDescentComputation])
      giraphConf.setAggregatorWriterClass(classOf[ConjugateGradientDescentAggregatorWriter])
      computation = classOf[ConjugateGradientDescentComputation].getCanonicalName
    }

    val result = GiraphJobManager.run("ia_giraph_cf",
      computation,
      config,
      giraphConf,
      context,
      "cf-learning-report_0")

    val resultsColumn = Column("cf_factors", DataTypes.vector(arguments.getNumFactors))
    frames.postSave(None, userFrame.toReference, new FrameSchema(List(frame.schema.column(arguments.userColName), resultsColumn)))
    frames.postSave(None, itemFrame.toReference, new FrameSchema(List(frame.schema.column(arguments.itemColName), resultsColumn)))

    CollaborativeFilteringResult(frames.expectFrame(userFrame.toReference), frames.expectFrame(itemFrame.toReference), result)
  }

}
