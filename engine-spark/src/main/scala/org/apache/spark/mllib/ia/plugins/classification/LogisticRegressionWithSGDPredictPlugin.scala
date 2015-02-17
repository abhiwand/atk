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

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.{ CreateEntityArgs, Naming }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameMeta }
import org.apache.spark.mllib.ia.plugins.classification.ClassificationWithSGDPredictArgs
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, SparkFrameData }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

class LogisticRegressionWithSGDPredictPlugin extends SparkCommandPlugin[ClassificationWithSGDPredictArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/predict"

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
  override def execute(arguments: ClassificationWithSGDPredictArgs)(implicit invocation: Invocation): UnitReturn =
    {
      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame.id)
      val modelMeta = models.expectModel(arguments.model.id)

      //create RDD from the frame
      val inputFrameRDD = frames.loadFrameData(sc, inputFrame)

      //Running MLLib
      val logRegJsObject = modelMeta.data.get
      val logRegData = logRegJsObject.convertTo[LogisticRegressionData]
      val logRegModel = logRegData.logRegModel
      if (arguments.observationColumns.isDefined) {
        require(logRegData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
      }
      val logRegColumns = arguments.observationColumns.getOrElse(logRegData.observationColumns)

      //predicting a label for the observation columns
      val predictionsRDD = inputFrameRDD.mapRows(row => {
        val array = row.valuesAsArray(logRegColumns)
        val doubles = array.map(i => DataTypes.toDouble(i))
        val point = Vectors.dense(doubles)
        val prediction = logRegModel.predict(point)
        row.addValue(prediction.toInt)
      })

      val updatedSchema = inputFrameRDD.frameSchema.addColumn("predicted_label", DataTypes.int32)
      val predictFrameRDD = new FrameRDD(updatedSchema, predictionsRDD)

      frames.saveFrameData(inputFrame.toReference, predictFrameRDD)
      new UnitReturn
    }

}
