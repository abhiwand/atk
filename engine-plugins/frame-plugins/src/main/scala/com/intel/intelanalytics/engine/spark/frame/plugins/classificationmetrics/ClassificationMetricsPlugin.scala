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

package com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ ClassificationMetricArgs, ClassificationMetricValue, FrameEntity }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.PythonRddStorage
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal

import scala.concurrent.ExecutionContext

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Computes Model accuracy, precision, recall, confusion matrix and f_measure
 */
class ClassificationMetricsPlugin extends SparkCommandPlugin[ClassificationMetricArgs, ClassificationMetricValue] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/classification_metrics"
  /**
   * Set the kryo class to use
   */
  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: ClassificationMetricArgs)(implicit invocation: Invocation) = 8
  /**
   * Computes Model accuracy, precision, recall, confusion matrix and f_measure
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationMetricArgs)(implicit invocation: Invocation): ClassificationMetricValue = {
    // dependencies (later to be replaced with dependency injection)

    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frameRef = arguments.frame
    val frameEntity = frames.expectFrame(arguments.frame)
    val frameSchema = frameEntity.schema
    val frameRdd = frames.loadLegacyFrameRdd(sc, frameRef)
    val betaValue = arguments.beta.getOrElse(1.0)
    val labelColumnIndex = frameSchema.columnIndex(arguments.labelColumn)
    val predColumnIndex = frameSchema.columnIndex(arguments.predColumn)

    // check if poslabel is an Int, string or None
    val metricsPoslabel: String = arguments.posLabel.isDefined match {
      case false => null
      case true => arguments.posLabel.get match {
        case Left(x) => x
        case Right(x) => x.toString
      }
    }
    // run the operation and return the results
    if (metricsPoslabel == null) {
      ClassificationMetrics.multiclassClassificationMetrics(frameRdd, labelColumnIndex, predColumnIndex, betaValue)
    }
    else {
      ClassificationMetrics.binaryClassificationMetrics(frameRdd, labelColumnIndex, predColumnIndex, metricsPoslabel, betaValue)
    }

  }
}
