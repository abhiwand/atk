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

package org.apache.spark.mllib.ia.plugins.classification

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.{ CreateEntityArgs, Naming }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameMeta }
import org.apache.spark.mllib.ia.plugins.classification.ClassificationWithSGDPredictArgs
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
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

      //create RDD from the frame
      val inputFrameRdd = frames.loadFrameData(sc, inputFrame)

      //Running MLLib
      val linRegJsObject = modelMeta.data.getOrElse(throw new RuntimeException("This model has not be trained yet. Please train before trying to predict"))
      val linRegData = linRegJsObject.convertTo[LinearRegressionData]
      val linRegModel = linRegData.linRegModel
      if (arguments.observationColumns.isDefined) {
        require(linRegData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
      }
      val linRegColumns = arguments.observationColumns.getOrElse(linRegData.observationColumns)

      //predicting a label for the observation columns
      val predictionsRDD = inputFrameRdd.mapRows(row => {
        val array = row.valuesAsArray(linRegColumns)
        val doubles = array.map(i => DataTypes.toDouble(i))
        val point = Vectors.dense(doubles)
        val prediction = linRegModel.predict(point)
        row.addValue(DataTypes.toFloat(prediction))
      })

      val updatedSchema = inputFrameRdd.frameSchema.addColumn("predicted_value", DataTypes.float32)
      val predictFrameRdd = new FrameRdd(updatedSchema, predictionsRDD)

      tryNew(CreateEntityArgs(description = Some("created by LinearRegressionWithSGDs predict operation"))) {
        newPredictedFrame: FrameMeta =>
          save(new SparkFrameData(newPredictedFrame.meta, predictFrameRdd))
      }.meta
    }

}
