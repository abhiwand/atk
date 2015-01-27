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
package org.apache.spark.mllib.ia.plugins.clustering

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.{ CreateEntityArgs, Naming }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.model.KMeansPredictArgs
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ FrameRDD, SparkFrameData }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors

import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._
import spray.json._

class KMeansPredictPlugin extends SparkCommandPlugin[KMeansPredictArgs, UnitReturn] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:k_means/predict"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */
  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Predict the cluster for each entry in a frame and return a new frame with existing columns and a predicted centroid's column",
    extendedSummary = Some("""
                             |
                             |    Parameters
                             |    ----------
                             |    predict_frame: Frame
                             |        Frame whose clusters are to be predicted
                             |
                             |    predict_for_observation_columns: List of strings
                             |        names of the observation columns
                             |
                             |
                             |    Returns
                             |    -------
                             |    Frame containing the original frame's columns and a column with the predicted cluster index
                             |
                             |
                             |    Examples
                             |    --------
                             |    ::
                             |
                             |        model = ia.KMeansModel(name='MyKMeansModel')
                             |        model.train(train_frame, ['name_of_observation_column1','name_of_observation_column2'])
                             |        new_frame = model.predict(predict_frame, ['name_of_observation_column1','name_of_observation_column2','name_of_observation_column3'])
                             |
                           """.stripMargin)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: KMeansPredictArgs)(implicit invocation: Invocation) = 1

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: KMeansPredictArgs)(implicit invocation: Invocation): UnitReturn = {

    val models = engine.models
    val frames = engine.frames

    val inputFrame = frames.expectFrame(arguments.frame.id)
    val modelMeta = models.expectModel(arguments.model.id)

    //create RDD from the frame
    val inputFrameRDD = frames.loadFrameData(sc, inputFrame)

    //Extracting the KMeansModel from the stored JsObject
    val kmeansJsObject = modelMeta.data.get
    val kmeansData = kmeansJsObject.convertTo[KMeansData]
    val kmeansModel = kmeansData.kMeansModel
    val kmeansColumns = arguments.observationColumns.getOrElse(kmeansData.observationColumns)

    //Predicting the cluster for each row
    val predictionsRDD = inputFrameRDD.mapRows(row => {
      val array = row.valuesAsArray(kmeansColumns)
      val doubles = array.map(i => DataTypes.toDouble(i))
      val point = Vectors.dense(doubles)
      val prediction = kmeansModel.predict(point)
      row.addValue(prediction)
    })

    val updatedSchema = inputFrameRDD.frameSchema.addColumn("predicted_cluster", DataTypes.int32)
    val predictFrameRDD = new FrameRDD(updatedSchema, predictionsRDD)

    frames.saveFrameData(inputFrame.toReference, predictFrameRDD)
    new UnitReturn
  }

}
