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

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.{ CreateEntityArgs, Naming }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameMeta }
import org.apache.spark.mllib.ia.plugins.classification.ClassificationWithSGDPredictArgs
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.SparkContext._
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

/**
 * Parameters
 * ----------
 * predict_frame : Frame
 *   A frame whose labels are to be predicted.
 *   By default, predict is run on the same columns over which the model is
 *   trained.
 *   The user could specify column names too if needed.
 * observation_column : list of str (Optional)
 *   Column(s) containing the observations whose labels are to be predicted.
 *   By default, we predict the labels over columns the LinearRegressionModel
 *   was trained on.
 */

@PluginDoc(oneLine = "Make new frame with column for label prediction.",
  extended = """Predict the labels for a test frame and create a new frame revision with
existing columns and a new predicted label's column.""",
  returns = "Frame containing the original frame's columns and a column with the predicted label.")
class LinearRegressionWithSGDPredictPlugin extends SparkCommandPlugin[ClassificationWithSGDPredictArgs, FrameEntity] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:linear_regression/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ClassificationWithSGDPredictArgs)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationWithSGDPredictArgs)(implicit invocation: Invocation): FrameEntity =
    {
      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame)
      val modelMeta = models.expectModel(arguments.model)

      //Running MLLib
      val linRegJsObject = modelMeta.data.getOrElse(throw new RuntimeException("This model has not be trained yet. Please train before trying to predict"))
      val linRegData = linRegJsObject.convertTo[LinearRegressionData]
      val linRegModel = linRegData.linRegModel
      if (arguments.observationColumns.isDefined) {
        require(linRegData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
      }
      val linRegColumns = arguments.observationColumns.getOrElse(linRegData.observationColumns)

      //create RDD from the frame
      val inputFrameRdd = frames.loadFrameData(sc, inputFrame)

      //predicting a label for the observation columns
      val predictionsRDD = inputFrameRdd.mapRows(row => {
        val array = row.valuesAsArray(linRegColumns)
        val doubles = array.map(i => DataTypes.toDouble(i))
        val point = Vectors.dense(doubles)
        val prediction = linRegModel.predict(point)
        row.addValue(DataTypes.toDouble(prediction))
      })

      val updatedSchema = inputFrameRdd.frameSchema.addColumn("predicted_value", DataTypes.float64)
      val predictFrameRdd = new FrameRdd(updatedSchema, predictionsRDD)

      tryNew(CreateEntityArgs(description = Some("created by LinearRegressionWithSGDs predict operation"))) {
        newPredictedFrame: FrameMeta =>
          save(new SparkFrameData(newPredictedFrame.meta, predictFrameRdd))
      }.meta
    }

}
