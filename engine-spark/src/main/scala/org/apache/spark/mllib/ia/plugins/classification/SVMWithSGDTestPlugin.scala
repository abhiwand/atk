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

package org.apache.spark.mllib.ia.plugins.classification

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.ClassificationMetricValue
import com.intel.intelanalytics.domain.model.ClassificationWithSGDArgs
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics.ClassificationMetrics
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.classification.{ SVMModel, LogisticRegressionModel, SVMWithSGD }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

/* Run the SVMWithSGD model on the test frame*/
class SVMWithSGDTestPlugin extends SparkCommandPlugin[ClassificationWithSGDArgs, ClassificationMetricValue] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:svm/test"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ClassificationWithSGDArgs)(implicit invocation: Invocation) = 9
  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationWithSGDArgs)(implicit invocation: Invocation): ClassificationMetricValue =
    {
      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame.id)
      val modelMeta = models.expectModel(arguments.model.id)

      //create RDD from the frame
      val testFrameRDD = frames.loadFrameData(sc, inputFrame)
      val labeledTestRDD: RDD[LabeledPoint] = testFrameRDD.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)

      //Running MLLib
      val svmJsObject = modelMeta.data.get
      val svmModel = svmJsObject.convertTo[SVMModel]

      //predicting and testing
      val scoreAndLabelRDD: RDD[Row] = labeledTestRDD.map { point =>
        val prediction = svmModel.predict(point.features)
        Array[Any](point.label, prediction)
      }

      //Run Binary classification metrics
      val posLabel: String = "1.0"
      ClassificationMetrics.binaryClassificationMetrics(scoreAndLabelRDD, 0, 1, posLabel, 1)

    }
}