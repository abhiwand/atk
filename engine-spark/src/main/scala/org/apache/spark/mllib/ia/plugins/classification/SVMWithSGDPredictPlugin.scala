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

import com.intel.intelanalytics.domain.{ CreateEntityArgs, Naming }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameMeta }
import com.intel.intelanalytics.domain.model.ModelPredict
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, SparkFrameData }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRow
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

class SVMWithSGDPredictPlugin extends SparkCommandPlugin[ModelPredict, FrameEntity] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:svm/predict"
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
                             |        model = ia.SvmModel(name='mySVM')
                             |        model.train(train_frame, ['name_of_observation_column'], 'name_of_label_column')
                             |        new_frame = model.predict(predict_frame, 'predict_for_observation_column')
                             |
                             |
                           """.stripMargin)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ModelPredict)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ModelPredict)(implicit invocation: Invocation): FrameEntity =
    {
      val models = engine.models
      val frames = engine.frames

      //validate arguments
      val frameId = arguments.frame.id
      val modelId = arguments.model.id

      val inputFrame = frames.expectFrame(frameId)
      val modelMeta = models.expectModel(modelId)

      //create RDD from the frame
      val predictFrameRDD = frames.loadFrameData(sc, inputFrame)
      val predictRowRDD: RDD[Row] = predictFrameRDD.toLegacyFrameRDD.rows

      val observationsVector = predictFrameRDD.toVectorRDD(List(arguments.observationColumn))

      //Running MLLib
      val logRegJsObject = modelMeta.data.get
      val logRegModel = logRegJsObject.convertTo[SVMModel]

      //predicting a label for the observation columns
      val labeledRDD: RDD[Row] = observationsVector.map { point =>
        val prediction = logRegModel.predict(point)
        Array[Any](prediction)
      }

      // Creating a new RDD with existing frame's entries and an additional column for predicted label
      val indexedPredictedRowRDD = predictRowRDD.zipWithIndex().map { case (x, y) => (y, x) }
      val indexedLabeledRowRDD = labeledRDD.zipWithIndex().map { case (x, y) => (y, x) }
      val result: RDD[sql.Row] = indexedPredictedRowRDD.join(indexedLabeledRowRDD).map { case (index, data) => new GenericRow(data._1 ++ data._2) }

      // Creating schema for the new frame
      val newSchema = predictFrameRDD.frameSchema.addColumn("predicted_label", DataTypes.float64)

      val outputFrameRDD = new FrameRDD(newSchema, result)

      tryNew(CreateEntityArgs(description = Some("created by SVM prediction command"))) { newPredictedFrame: FrameMeta =>
        save(new SparkFrameData(newPredictedFrame.meta, outputFrameRDD))
      }.meta
    }

}