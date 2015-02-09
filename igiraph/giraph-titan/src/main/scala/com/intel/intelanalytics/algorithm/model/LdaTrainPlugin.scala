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

package com.intel.intelanalytics.algorithm.model

import com.intel.giraph.algorithms.lda.CVB0LDAComputation
import com.intel.giraph.algorithms.lda.CVB0LDAComputation.{ CVB0LDAAggregatorWriter, CVB0LDAMasterCompute }
import com.intel.ia.giraph.lda.v2._
import com.intel.intelanalytics.algorithm.util.{ GiraphConfigurationUtil, GiraphJobManager }
import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.schema.{ DataTypes, Column, FrameSchema }
import com.intel.intelanalytics.engine.plugin.{ CommandInvocation, CommandPlugin, Invocation }
import org.apache.spark.sql.parquet.ia.giraph.frame.{ LdaParquetFrameEdgeInputFormat, LdaParquetFrameVertexOutputFormat }
import spray.json._
import LdaJsonFormat._

/**
 * Latent Dirichlet allocation
 */
class LdaTrainPlugin
    extends CommandPlugin[LdaTrainArgs, LdaTrainResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lda/train"

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

    //giraphConf.setVertexInputFormatClass(classOf[LdaParquetFrameVertexValueInputFormat])
    giraphConf.setEdgeInputFormatClass(classOf[LdaParquetFrameEdgeInputFormat])
    giraphConf.setVertexOutputFormatClass(classOf[LdaParquetFrameVertexOutputFormat])
    giraphConf.setMasterComputeClass(classOf[CVB0LDAMasterCompute])
    giraphConf.setComputationClass(classOf[CVB0LDAComputation])
    giraphConf.setAggregatorWriterClass(classOf[CVB0LDAAggregatorWriter])

    val report = GiraphJobManager.run(s"ia_giraph_lda_train_${invocation.asInstanceOf[CommandInvocation].commandId}",
      classOf[CVB0LDAComputation].getCanonicalName,
      config, giraphConf, invocation, "lda-learning-report_0")

    val resultsColumn = Column("lda_results", DataTypes.str)

    frames.postSave(None, docOut.toReference, new FrameSchema(List(frame.schema.column(arguments.documentColumnName), resultsColumn)))
    frames.postSave(None, wordOut.toReference, new FrameSchema(List(frame.schema.column(arguments.wordColumnName), resultsColumn)))

    LdaTrainResult(frames.expectFrame(docOut.id), frames.expectFrame(wordOut.id), report)
  }

}
