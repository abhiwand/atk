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

package org.apache.spark.mllib.classification.ia.plugins

import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.model.{ ModelMeta, ModelLoad, Model }
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, FrameRDD }
import com.intel.intelanalytics.domain.frame.ClassificationMetricValue
import com.intel.intelanalytics.domain.model.ModelLoad
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics.ClassificationMetrics
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin, SparkInvocation }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import MLLibJsonProtocol._
import scala.concurrent.ExecutionContext

import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

/* Run the LogisticRegressionWithSGD model on the test frame*/
class LogisticRegressionWithSGDTestPlugin extends SparkCommandPlugin[ModelLoad, ClassificationMetricValue] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/test"
  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Predict the labels for a test frame and run classification metrics on predicted and target labels",
    extendedSummary = Some("""

    Parameters
    ----------
    frame: Frame
        frame whose labels are to be predicted
    observation_column: str
        column containing the observations
    label_column: str
        column containing the actual label for each observation

    Returns
    -------
    object
    <object>.accuracy : double
    <object>.confusion_matrix : table
    <object>.f_measure : double
    <object>.precision : double
    <object>.recall : double


    Examples
    --------
                             |model = ia.LogisticRegressionModel(name='LogReg')
                             |model.train(train_frame, 'name_of_observation_column', 'name_of_label_column')
                             |metrics = model.test(test_frame,'name_of_observation_column', 'name_of_label_column')
                             |
                             |metrics.f_measure
                             |0.66666666666666663
                             |
                             |metrics.recall
                             |0.5
                             |
                             |metrics.accuracy
                             |0.75
                             |
                             |metrics.precision
                             |1.0
                             |
                             |metrics.confusion_matrix
                             |
                             |             Predicted
                             |             _pos_ _neg__
                             |Actual  pos |  1     1
                             |        neg |  0     2
                             |
                           """.stripMargin)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ModelLoad)(implicit invocation: Invocation) = 9
  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ModelLoad)(implicit invocation: Invocation): ClassificationMetricValue =
    {

      val models = engine.models
      val frames = engine.frames

      //validate arguments
      val frameId = arguments.frame.id
      val modelId = arguments.model.id

      val inputFrame = frames.expectFrame(frameId)
      val modelMeta = models.expectModel(modelId)

      //create RDD from the frame
      val testFrameRDD = frames.loadFrameData(sc, inputFrame)
      val labeledTestRDD: RDD[LabeledPoint] = testFrameRDD.toLabeledPointRDD(arguments.labelColumn, List(arguments.observationColumn))

      //Running MLLib
      val logRegJsObject = modelMeta.data.get
      val logRegModel = logRegJsObject.convertTo[LogisticRegressionModel]

      //predicting and testing
      val scoreAndLabelRDD: RDD[Row] = labeledTestRDD.map { point =>
        val prediction = logRegModel.predict(point.features)
        Array[Any](point.label, prediction)
      }

      //Run Binary classification metrics
      val posLabel: String = "1.0"
      ClassificationMetrics.binaryClassificationMetrics(scoreAndLabelRDD, 0, 1, posLabel, 1)

    }
}
