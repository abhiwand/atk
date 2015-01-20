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

import com.intel.intelanalytics.domain.Naming
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameMeta }
import com.intel.intelanalytics.domain.model.LogisticRegressionWithSGDPredictArgs
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

class LogisticRegressionWithSGDPredictPlugin extends SparkCommandPlugin[LogisticRegressionWithSGDPredictArgs, FrameEntity] {
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

  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Predict the labels for a test frame and return a new frame with existing columns and a predicted label's column",
    extendedSummary = Some("""
                             |
                             |    Parameters
                             |    ----------
                             |    predict_frame: Frame
                             |        Frame whose labels are to be predicted
                             |
                             |    predict_for_observation_column: str
                             |        column containing the observations
                             |
                             |
                             |    Returns
                             |    -------
                             |    Frame containing the original frame's columns and a column with the predicted label
                             |
                             |
                             |    Examples
                             |    --------
                             |    ::
                             |
                             |        model = ia.LogisticRegressionModel(name='LogReg')
                             |        model.train(train_frame, 'name_of_observation_column', 'name_of_label_column')
                             |        new_frame = model.predict(predict_frame, 'predict_for_observation_column')
                             |
                             |
                           """.stripMargin)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: LogisticRegressionWithSGDPredictArgs)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LogisticRegressionWithSGDPredictArgs)(implicit invocation: Invocation): FrameEntity =
    {
      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame.id)
      val modelMeta = models.expectModel(arguments.model.id)

      //create RDD from the frame
      val inputFrameRDD = frames.loadFrameData(sc, inputFrame)

      //Running MLLib
      val logRegJsObject = modelMeta.data.get
      val logRegModel = logRegJsObject.convertTo[LogisticRegressionModel]

      //predicting a label for the observation columns
      val predictionsRDD = inputFrameRDD.mapRows(row => {
        val array = row.valuesAsArray(arguments.observationColumns)
        val doubles = array.map(i => DataTypes.toDouble(i))
        val point = Vectors.dense(doubles)
        val prediction = logRegModel.predict(point)
        row.addValue(prediction)
      })

      val updatedSchema = inputFrameRDD.frameSchema.addColumn("predicted_label", DataTypes.float64)
      val predictFrameRDD = new FrameRDD(updatedSchema, predictionsRDD)

      tryNew(Some(Naming.generateName(Some("predicted_frame")))) {
        newPredictedFrame: FrameMeta =>
          save(new SparkFrameData(newPredictedFrame.meta, predictFrameRDD))
      }.meta
    }

}
