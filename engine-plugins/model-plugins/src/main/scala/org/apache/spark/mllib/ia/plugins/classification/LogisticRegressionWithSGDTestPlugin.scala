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

package org.apache.spark.mllib.ia.plugins.classification

import com.intel.intelanalytics.domain.frame.ClassificationMetricValue
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics.ClassificationMetrics
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

/**
 * Parameters
 * ----------
 * predict_frame : Frame
 *   A frame whose labels are to be predicted.
 * label_column : str
 *   Column containing the actual label for each observation.
 * observation_column : list of str (Optional)
 *   Column(s) containing the observations whose labels are to be predicted and
 *   tested.
 *   By default, we test over the columns the LogisticRegressionModel was
 *   trained on.
 */

/* Run the LogisticRegressionWithSGD model on the test frame*/
@PluginDoc(oneLine = "Predict test frame labels and show metrics.",
  extended = "Predict the labels for a test frame and run classification metrics on predicted and target labels.",
  returns = """object
    An object with binary classification metrics.
    The data returned is composed of multiple components:
<object>.accuracy : double
<object>.confusion_matrix : table
<object>.f_measure : double
<object>.precision : double
<object>.recall : double""")
class LogisticRegressionWithSGDTestPlugin extends SparkCommandPlugin[ClassificationWithSGDTestArgs, ClassificationMetricValue] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/test"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)
  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ClassificationWithSGDTestArgs)(implicit invocation: Invocation) = 9
  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationWithSGDTestArgs)(implicit invocation: Invocation): ClassificationMetricValue =
    {

      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame)
      val modelMeta = models.expectModel(arguments.model)

      //create RDD from the frame
      val testFrameRdd = frames.loadFrameData(sc, inputFrame)

      val logRegJsObject = modelMeta.data.get
      val logRegData = logRegJsObject.convertTo[LogisticRegressionData]
      val logRegModel = logRegData.logRegModel
      if (arguments.observationColumns.isDefined) {
        require(logRegData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and test should be same")
      }
      val logRegColumns = arguments.observationColumns.getOrElse(logRegData.observationColumns)

      val labeledTestRdd: RDD[LabeledPoint] = testFrameRdd.toLabeledPointRDD(arguments.labelColumn, logRegColumns)

      //predicting and testing
      val scoreAndLabelRdd: RDD[Row] = labeledTestRdd.map { point =>
        val prediction = logRegModel.predict(point.features)
        Array[Any](point.label, prediction)
      }

      //Run Binary classification metrics
      val posLabel: String = "1.0"
      ClassificationMetrics.binaryClassificationMetrics(scoreAndLabelRdd, 0, 1, posLabel, 1)

    }
}
