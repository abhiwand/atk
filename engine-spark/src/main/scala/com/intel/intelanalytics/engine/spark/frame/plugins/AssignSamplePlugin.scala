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

package com.intel.intelanalytics.engine.spark.frame.plugins

import com.intel.intelanalytics.domain.frame.{ FrameReference, FrameEntity }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.spark.mllib.util.{ LabeledLine, MLDataSplitter }
import org.apache.spark.rdd.RDD

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.sql
import spray.json._

case class AssignSampleArgs(frame: FrameReference,
                            samplePercentages: List[Double],
                            sampleLabels: Option[List[String]] = None,
                            outputColumn: Option[String] = None,
                            randomSeed: Option[Int] = None) {
  require(frame != null, "AssignSample requires a non-null dataframe.")

  require(samplePercentages != null, "AssignSample requires that the percentages vector be non-null.")

  require(samplePercentages.forall(_ >= 0.0d), "AssignSample requires that all percentages be non-negative.")
  require(samplePercentages.forall(_ <= 1.0d), "AssignSample requires that all percentages be no more than 1.")

  val sumOfPercentages = samplePercentages.reduce(_ + _)

  require(sumOfPercentages > 1.0d - 0.000000001, "AssignSample:  Sum of provided probabilities falls below one.")
  require(sumOfPercentages < 1.0d + 0.000000001, "AssignSample:  Sum of provided probabilities exceeds one.")

  def splitLabels: Array[String] = if (sampleLabels.isEmpty) {
    if (samplePercentages.length == 3) {
      Array("TR", "TE", "VA")
    }
    else {
      (0 to samplePercentages.length - 1).map(i => "Sample#" + i).toArray
    }
  }
  else {
    sampleLabels.get.toArray
  }
}

/** Json conversion for arguments case class */
object AssignSampleJsonFormat {
  implicit val CCFormat = jsonFormat5(AssignSampleArgs)
}

import AssignSampleJsonFormat._

/**
 * Assign classes to rows.
 */
class AssignSamplePlugin extends SparkCommandPlugin[AssignSampleArgs, FrameEntity] {

  override def name: String = "frame/assign_sample"

  /**
   * Assign classes to rows.
   *
   * @param arguments command arguments
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AssignSampleArgs)(implicit invocation: Invocation): FrameEntity = {

    val frames = engine.frames
    val ctx = sc

    val frameID = arguments.frame.id
    val frame = frames.expectFrame(frameID)
    val splitPercentages = arguments.samplePercentages.toArray

    val outputColumn = arguments.outputColumn.getOrElse("sample_bin")
    if (frame.schema.hasColumn(outputColumn))
      throw new IllegalArgumentException(s"Duplicate column name: $outputColumn")
    val seed = arguments.randomSeed.getOrElse(0)

    // run the operation
    val splitter = new MLDataSplitter(splitPercentages, arguments.splitLabels, seed)
    val labeledRDD: RDD[LabeledLine[String, sql.Row]] = splitter.randomlyLabelRDD(frames.loadFrameData(ctx, frame))

    val splitRDD: RDD[Rows.Row] =
      labeledRDD.map((x: LabeledLine[String, sql.Row]) => x.entry.asInstanceOf[Array[Any]] :+ x.label.asInstanceOf[Any])

    val updatedSchema = frame.schema.addColumn(outputColumn, DataTypes.string)

    // save results
    frames.saveFrameData(frame.toReference, FrameRDD.toFrameRDD(updatedSchema, splitRDD))
  }
}
