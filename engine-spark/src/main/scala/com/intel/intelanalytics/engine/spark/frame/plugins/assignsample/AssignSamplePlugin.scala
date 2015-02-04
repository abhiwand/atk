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

package com.intel.intelanalytics.engine.spark.frame.plugins.assignsample

import com.intel.intelanalytics.domain.frame.{ AssignSampleArgs, FrameEntity }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Assign classes to rows.
 */
class AssignSamplePlugin extends SparkCommandPlugin[AssignSampleArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/assign_sample"

  /**
   * Assign classes to rows.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AssignSampleArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)

    val frames = engine.frames
    val ctx = sc

    val frame = frames.expectFrame(arguments.frame.id)
    val samplePercentages = arguments.samplePercentages.toArray

    val outputColumnName = arguments.outputColumnName

    if (frame.schema.hasColumn(outputColumnName))
      throw new IllegalArgumentException(s"Duplicate column name: $outputColumnName")

    // run the operation
    val splitter = new MLDataSplitter(samplePercentages, arguments.splitLabels, arguments.seed)
    val labeledRDD: RDD[LabeledLine[String, sql.Row]] = splitter.randomlyLabelRDD(frames.loadFrameData(ctx, frame))

    val splitRDD: RDD[Rows.Row] =
      labeledRDD.map((x: LabeledLine[String, sql.Row]) => (x.entry.asInstanceOf[Seq[Any]] :+ x.label.asInstanceOf[Any]).toArray[Any])

    val updatedSchema = frame.schema.addColumn(outputColumnName, DataTypes.string)

    // save results
    frames.saveFrameData(frame.toReference, FrameRDD.toFrameRDD(updatedSchema, splitRDD))
  }
}
