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
