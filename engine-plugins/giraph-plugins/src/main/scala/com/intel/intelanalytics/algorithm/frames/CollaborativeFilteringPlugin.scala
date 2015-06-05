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
