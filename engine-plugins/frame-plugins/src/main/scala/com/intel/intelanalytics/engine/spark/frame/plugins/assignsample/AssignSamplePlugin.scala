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

package com.intel.intelanalytics.engine.spark.frame.plugins.assignsample

import com.intel.intelanalytics.domain.frame.{ AssignSampleArgs, FrameEntity }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
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

    val frame = frames.expectFrame(arguments.frame)
    val samplePercentages = arguments.samplePercentages.toArray

    val outputColumnName = arguments.outputColumnName

    require(!frame.schema.hasColumn(outputColumnName), s"Duplicate column name: $outputColumnName")

    // run the operation
    val splitter = new MLDataSplitter(samplePercentages, arguments.splitLabels, arguments.seed)
    val labeledRDD: RDD[LabeledLine[String, sql.Row]] = splitter.randomlyLabelRDD(frames.loadFrameData(sc, frame))

    val splitRDD: RDD[Rows.Row] =
      labeledRDD.map((x: LabeledLine[String, sql.Row]) => (x.entry.asInstanceOf[Seq[Any]] :+ x.label.asInstanceOf[Any]).toArray[Any])

    val updatedSchema = frame.schema.addColumn(outputColumnName, DataTypes.string)

    // save results
    frames.saveFrameData(frame.toReference, FrameRdd.toFrameRdd(updatedSchema, splitRDD))
  }
}
