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

import com.intel.giraph.algorithms.lp.LabelPropagationComputation
import com.intel.giraph.algorithms.lp.LabelPropagationComputation.{ LabelPropagationAggregatorWriter, LabelPropagationMasterCompute }
import com.intel.ia.giraph.lp._
import com.intel.intelanalytics.algorithm.util.{ GiraphConfigurationUtil, GiraphJobManager }
import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.schema.{ Column, FrameSchema }
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, Invocation }
import org.apache.spark.sql.parquet.ia.giraph.frame._

import LabelPropagationJsonFormat._

class LabelPropagationPlugin
    extends CommandPlugin[LabelPropagationArgs, LabelPropagationResult] {

  /**
   * The name of the command, e.g. frame:/label_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/label_propagation"

  override def execute(arguments: LabelPropagationArgs)(implicit context: Invocation): LabelPropagationResult = {

    val frames = engine.frames
    val config = configuration

    //TODO validate frame args here
    val frame = frames.expectFrame(arguments.frame)
    require(frame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    // setup and run
    val hadoopConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(config, "giraph")
    val giraphConf = new LabelPropagationConfiguration(hadoopConf)

    //    GiraphConfigurationUtil.set(hadoopConf, "lp.core", arguments.maxIterations)
    //    GiraphConfigurationUtil.set(hadoopConf, "lp.convergenceThreshold", arguments.convergenceThreshold)
    //    GiraphConfigurationUtil.set(hadoopConf, "lp.anchorThreshold", arguments.anchorThreshold)
    //    GiraphConfigurationUtil.set(hadoopConf, "lp.bidirectionalCheck", arguments.validateGraphStructure)
    //
    //    GiraphConfigurationUtil.set(hadoopConf, "giraphjob.maxSteps", arguments.maxIterations)
    //    GiraphConfigurationUtil.set(hadoopConf, "input.vertex.value.property.key.list", Some(arguments.vertexValuePropertyList.mkString(argSeparator)))
    //    GiraphConfigurationUtil.set(hadoopConf, "input.edge.value.property.key.list", Some(arguments.edgeValuePropertyList.mkString(argSeparator)))
    //    GiraphConfigurationUtil.set(hadoopConf, "input.edge.label.list", Some(arguments.inputEdgeLabelList.mkString(argSeparator)))
    //    GiraphConfigurationUtil.set(hadoopConf, "output.vertex.property.key.list", Some(arguments.outputVertexPropertyList.mkString(argSeparator)))
    //    GiraphConfigurationUtil.set(hadoopConf, "vector.value", Some(arguments.vectorValue.toString))

    //    giraphConf.setVertexInputFormatClass(classOf[TitanVertexInputFormatPropertyGraph4LP])
    //    giraphConf.setVertexOutputFormatClass(classOf[TitanVertexOutputFormatPropertyGraph4LP[_ <: org.apache.hadoop.io.LongWritable, _ <: com.intel.giraph.io.VertexData4LPWritable, _ <: org.apache.hadoop.io.Writable]])

    val outputFrame = frames.prepareForSave(CreateEntityArgs(description = Some("Label propagation results")))
    val inputFormatConfig = new LabelPropagationInputFormatConfig(frame.storageLocation.get, frame.schema)
    val outputFormatConfig = new LabelPropagationOutputFormatConfig(outputFrame.storageLocation.get)
    val labelPropagationConfig = new LabelPropagationConfig(inputFormatConfig, outputFormatConfig, arguments)

    giraphConf.setConfig(labelPropagationConfig)
    GiraphConfigurationUtil.set(giraphConf, "giraphjob.maxSteps", arguments.maxIterations)

    giraphConf.setEdgeInputFormatClass(classOf[LabelPropagationEdgeInputFormat])
    giraphConf.setVertexOutputFormatClass(classOf[LabelPropagationVertexOutputFormat])
    giraphConf.setVertexInputFormatClass(classOf[LabelPropagationVertexInputFormat])
    giraphConf.setMasterComputeClass(classOf[LabelPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LabelPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LabelPropagationAggregatorWriter])

    val result = GiraphJobManager.run("ia_giraph_lp",
      classOf[LabelPropagationComputation].getCanonicalName,
      config,
      giraphConf,
      context,
      "lp-learning-report_0")

    val resultsColumn = Column("lp_results", frame.schema.columnDataType(arguments.srcLabelColName))
    frames.postSave(None, outputFrame.toReference, new FrameSchema(List(frame.schema.column(arguments.srcColName), resultsColumn)))

    LabelPropagationResult(result)

  }

}
