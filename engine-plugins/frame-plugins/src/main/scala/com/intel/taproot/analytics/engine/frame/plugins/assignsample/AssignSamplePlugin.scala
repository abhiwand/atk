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

package com.intel.taproot.analytics.engine.frame.plugins.assignsample

import com.intel.taproot.analytics.domain.frame.{ AssignSampleArgs, FrameEntity }
import com.intel.taproot.analytics.domain.schema.DataTypes
import com.intel.taproot.analytics.engine.frame.plugins.{ LabeledLine, MLDataSplitter }
import org.apache.spark.sql.Row
import com.intel.taproot.analytics.engine.frame.SparkFrame
import com.intel.taproot.analytics.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

/**
 * Assign classes to rows.
 */
@PluginDoc(oneLine = "Randomly group rows into user-defined classes.",
  extended = """Randomly assign classes to rows given a vector of percentages.
The table receives an additional column that contains a random label.
The random label is generated by a probability distribution function.
The distribution function is specified by the sample_percentages, a list of
floating point values, which add up to 1.
The labels are non-negative integers drawn from the range
:math:`[ 0, len(S) - 1]` where :math"`S` is the sample_percentages.
Optionally, the user can specify a list of strings to be used as the labels.
If the number of labels is 3, the labels will default to "TR", "TE" and "VA".

Notes
-----
**Probability Validation**

The sample percentages provided by the user are preserved to at least eight
decimal places, but beyond this there may be small changes due to floating
point imprecision.

In particular:

1)  The engine validates that the sum of probabilities sums to 1.0 within
    eight decimal places and returns an error if the sum falls outside of this
    range.
2)  The probability of the final class is clamped so that each row receives a
    valid label with probability one.""")
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
    val frame: SparkFrame = arguments.frame
    val samplePercentages = arguments.samplePercentages.toArray

    // run the operation
    val splitter = new MLDataSplitter(samplePercentages, arguments.splitLabels, arguments.seed)
    val labeledRDD: RDD[LabeledLine[String, Row]] = splitter.randomlyLabelRDD(frame.rdd)

    val splitRDD: RDD[Array[Any]] = labeledRDD.map((x: LabeledLine[String, Row]) =>
      (x.entry.toSeq :+ x.label.asInstanceOf[Any]).toArray[Any]
    )

    val updatedSchema = frame.schema.addColumn(arguments.outputColumnName, DataTypes.string)
    frame.save(FrameRdd.toFrameRdd(updatedSchema, splitRDD))
  }
}
