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

import com.intel.graphbuilder.driver.spark.titan.examples.ExamplesUtils
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ DataFrameTemplate, ClassificationMetricValue, DataFrame }
import com.intel.intelanalytics.domain.model.{ ModelPredict, ModelLoad }
import com.intel.intelanalytics.domain.schema.{ FrameSchema, DataTypes, Column }
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

import scala.concurrent.ExecutionContext

class LogisticRegressionWithSGDPredictPlugin extends SparkCommandPlugin[ModelPredict, DataFrame] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/predict"
  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Predict the labels for a test frame and return a new frame with existing columns and a predicted labels column",
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
  Frame

  Examples
  -----
  model = ia.LogisticRegressionModel(name='LogReg')
  model.train(train_frame, 'name_of_observation_column', 'name_of_label_column')
  new_frame = model.predict(train_frame, 'name_of_observation_column')
  """)))
  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ModelPredict) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @param user current user
   * @return a value of type declared as the Return type.
   */
  override def execute(invocation: SparkInvocation, arguments: ModelPredict)(implicit user: UserPrincipal, executionContext: ExecutionContext): DataFrame =
    {
      val models = invocation.engine.models
      val frames = invocation.engine.frames
      val fsRoot = invocation.engine.fsRoot
      val ctx = invocation.sparkContext

      //validate arguments
      val frameId = arguments.frame.id
      val modelId = arguments.model.id

      val inputFrame = frames.expectFrame(frameId)
      val modelMeta = models.expectModel(modelId)

      //create RDD from the frame
      val testFrameRDD = frames.loadFrameRDD(ctx, inputFrame)
      val testRowRDD: RDD[Row] = testFrameRDD.toLegacyFrameRDD.rows

      //TODO: create
      //val labeledTestRDD: RDD[LabeledPoint] = testFrameRDD.toLabeledPointRDD(List(arguments.observationColumn))
      val observationsVector = testFrameRDD.toVectorRDD(List(arguments.observationColumn))
      val x = Array(observationsVector)
      //Running MLLib
      val logRegJsObject = modelMeta.data.get
      val logRegModel = logRegJsObject.convertTo[LogisticRegressionModel]

      //predicting
      val LabeledRDD: RDD[Row] = observationsVector.map { point =>
        val prediction = logRegModel.predict(point)
        Array[Any](prediction)
      }

      //creating a new frame
      val newPredictedFrame = frames.create(DataFrameTemplate(generatePredictedFrameName(arguments.frame.id.toString), None))

      val indexedTestRowRDD = testRowRDD.zipWithIndex().map { case (x, y) => (y, x) }
      val indexedLabeledRDD = LabeledRDD.zipWithIndex().map { case (x, y) => (y, x) }

      val result = indexedTestRowRDD.join(indexedLabeledRDD).map { case (index, data) => data._1 ++ data._2 } map (s => new GenericRow(Array[Any](s)).asInstanceOf[sql.Row])
      // Take original rowCount
      val rowCount = result.count

      val newSchema = testFrameRDD.frameSchema.addColumn("predicted_label", DataTypes.str)
      //println("\n @@@3 newSchema's column names:" +newSchema.columnNamesAsString)
      //val newColumns = List(Column("predicted_label", DataTypes.str))

      println("\n @@@3 result:" + result.toString() + "::: " + result.count())
      val outputFrameRDD = new FrameRDD(newSchema, result)
      println("\n @@@4 outputFrameRDD's schema: " + outputFrameRDD.frameSchema.columnNamesAsString)

      frames.saveFrame(newPredictedFrame, outputFrameRDD, Some(rowCount))

    }

  def generatePredictedFrameName(originalFrame: String): String = {
    "predicted_frame_" + originalFrame
  }
}
