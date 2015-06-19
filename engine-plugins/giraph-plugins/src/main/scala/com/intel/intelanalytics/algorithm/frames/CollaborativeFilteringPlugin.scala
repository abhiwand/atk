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
import org.apache.spark.sql.parquet.ia.giraph.frame.cf.{ CollaborativeFilteringVertexOutputFormat, CollaborativeFilteringEdgeInputFormat }
import CollaborativeFilteringJsonFormat._

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
    if (CollaborativeFilteringConstants.alsAlgorithm.equalsIgnoreCase(collaborativeFilteringConfig.evaluationFunction)) {
      giraphConf.setVertexOutputFormatClass(classOf[CollaborativeFilteringVertexOutputFormat[VertexData4CFWritable]])
      giraphConf.setMasterComputeClass(classOf[AlternatingLeastSquaresMasterCompute])
      giraphConf.setComputationClass(classOf[AlternatingLeastSquaresComputation])
      giraphConf.setAggregatorWriterClass(classOf[AlternatingLeastSquaresAggregatorWriter])
      computation = classOf[AlternatingLeastSquaresComputation].getCanonicalName
    }
    else if (CollaborativeFilteringConstants.cgdAlgorithm.equalsIgnoreCase(collaborativeFilteringConfig.evaluationFunction)) {
      giraphConf.setVertexOutputFormatClass(classOf[CollaborativeFilteringVertexOutputFormat[VertexData4CGDWritable]])
      giraphConf.setMasterComputeClass(classOf[ConjugateGradientDescentMasterCompute])
      giraphConf.setComputationClass(classOf[ConjugateGradientDescentComputation])
      giraphConf.setAggregatorWriterClass(classOf[ConjugateGradientDescentAggregatorWriter])
      computation = classOf[ConjugateGradientDescentComputation].getCanonicalName
    }
    else {
      throw new NotImplementedError("only als & cgd algorithms are supported")
    }

    val result = GiraphJobManager.run("ia_giraph_cf",
      computation,
      config,
      giraphConf,
      context,
      CollaborativeFilteringConstants.reportFilename)

    val resultsColumn = Column("cf_factors", DataTypes.vector(arguments.getNumFactors))
    frames.postSave(None, userFrame.toReference, new FrameSchema(List(frame.schema.column(arguments.userColName), resultsColumn)))
    frames.postSave(None, itemFrame.toReference, new FrameSchema(List(frame.schema.column(arguments.itemColName), resultsColumn)))

    CollaborativeFilteringResult(frames.expectFrame(userFrame.toReference), frames.expectFrame(itemFrame.toReference), result)
  }

}
