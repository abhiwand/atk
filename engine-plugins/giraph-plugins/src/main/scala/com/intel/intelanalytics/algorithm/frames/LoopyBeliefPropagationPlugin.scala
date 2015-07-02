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

import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation
import com.intel.giraph.algorithms.lbp.LoopyBeliefPropagationComputation.{ LoopyBeliefPropagationAggregatorWriter, LoopyBeliefPropagationMasterCompute }
import com.intel.ia.giraph.lbp._
import com.intel.intelanalytics.algorithm.util.{ GiraphConfigurationUtil, GiraphJobManager }
import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.schema.{ FrameSchema, Column }
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import org.apache.spark.sql.parquet.ia.giraph.frame.lbp.{ LoopyBeliefPropagationVertexInputFormat, LoopyBeliefPropagationVertexOutputFormat, LoopyBeliefPropagationEdgeInputFormat }

import LoopyBeliefPropagationJsonFormat._

class LoopyBeliefPropagationPlugin
    extends CommandPlugin[LoopyBeliefPropagationArgs, LoopyBeliefPropagationResult] {

  /**
   * The name of the command, e.g. frame:/label_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/loopy_belief_propagation"

  override def execute(arguments: LoopyBeliefPropagationArgs)(implicit invocation: Invocation): LoopyBeliefPropagationResult = {
    val frames = engine.frames
    val config = configuration

    //TODO validate frame args here
    val frame = frames.expectFrame(arguments.frame)
    require(frame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    // setup and run
    val hadoopConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val giraphConf = new LoopyBeliefPropagationConfiguration(hadoopConf)

    val outputFrame = frames.prepareForSave(CreateEntityArgs(description = Some("Loopy belief propagation results")))
    val inputFormatConfig = new LoopyBeliefPropagationInputFormatConfig(frame.storageLocation.get, frame.schema)
    val outputFormatConfig = new LoopyBeliefPropagationOutputFormatConfig(outputFrame.storageLocation.get)
    val loopyBeliefPropagationConfig = new LoopyBeliefPropagationConfig(inputFormatConfig, outputFormatConfig, arguments)

    giraphConf.setConfig(loopyBeliefPropagationConfig)
    GiraphConfigurationUtil.set(giraphConf, "giraphjob.maxSteps", arguments.maxIterations)
    GiraphConfigurationUtil.set(giraphConf, "mapreduce.input.fileinputformat.inputdir", Some(inputFormatConfig.parquetFileLocation))

    giraphConf.setEdgeInputFormatClass(classOf[LoopyBeliefPropagationEdgeInputFormat])
    giraphConf.setVertexOutputFormatClass(classOf[LoopyBeliefPropagationVertexOutputFormat])
    giraphConf.setVertexInputFormatClass(classOf[LoopyBeliefPropagationVertexInputFormat])
    giraphConf.setMasterComputeClass(classOf[LoopyBeliefPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LoopyBeliefPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LoopyBeliefPropagationAggregatorWriter])

    val result = GiraphJobManager.run("ia_giraph_lbp",
      classOf[LoopyBeliefPropagationComputation].getCanonicalName,
      config,
      giraphConf,
      invocation,
      "lbp-learning-report")

    val resultsColumn = Column(arguments.srcLabelColName, frame.schema.columnDataType(arguments.srcLabelColName))
    frames.postSave(None, outputFrame.toReference, new FrameSchema(List(frame.schema.column(arguments.srcColName), resultsColumn)))

    LoopyBeliefPropagationResult(frames.expectFrame(outputFrame.toReference), result)
  }

}
