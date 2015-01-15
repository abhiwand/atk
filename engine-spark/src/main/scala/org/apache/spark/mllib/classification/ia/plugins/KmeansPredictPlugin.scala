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
import com.intel.intelanalytics.domain.Naming
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.{ FrameMeta, DataFrameTemplate, ClassificationMetricValue, DataFrame }
import com.intel.intelanalytics.domain.model.{ KmeansModelPredict, ModelLoad }
import com.intel.intelanalytics.domain.schema.{ FrameSchema, DataTypes, Column }
import com.intel.intelanalytics.engine.Rows
import com.intel.intelanalytics.engine.Rows.Row
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData, FrameRDD }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkInvocation, SparkCommandPlugin }
import com.intel.intelanalytics.security.UserPrincipal
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

import scala.concurrent.ExecutionContext

class KmeansPredictPlugin extends SparkCommandPlugin[KmeansModelPredict, DataFrame] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:kmeans/predict"

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  override def doc: Option[CommandDoc] = Some(CommandDoc(oneLineSummary = "Predict the cluster for each entry in a frame and return a new frame with existing columns and a predicted centroid's column",
    extendedSummary = Some(
      """
                              |
                           """.stripMargin)))

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: KmeansModelPredict)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: KmeansModelPredict)(implicit invocation: Invocation): DataFrame = {
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

    val vectorRDD = predictFrameRDD.mapRows(row => {
      val array = row.valuesAsArray(arguments.observationColumns)
      val b = array.map(i => DataTypes.toDouble(i))
      Vectors.dense(b)
    })

    //Running MLLib
    val kMeansJsObject = modelMeta.data.get
    val kmeansModel = kMeansJsObject.convertTo[KMeansModel]

    //predicting a label for the observation columns
      val labeledRDD: RDD[Row] = vectorRDD.map { point =>
            val prediction = kmeansModel.predict(point)
            Array[Any](prediction)
    }
    //val predictionsRDD = kmeansModel.predict(vectorRDD)
      // Creating a new RDD with existing frame's entries and an additional column for predicted cluster
    val indexedPredictionsRDD = labeledRDD.zipWithIndex().map { case (x, y) => (y, x)}
    val indexedLabeledRowRDD =predictRowRDD.zipWithIndex().map { case (x, y) => (y, x)}
    val result: RDD[sql.Row] = indexedLabeledRowRDD.join(indexedPredictionsRDD).map { case (index, data) => new GenericRow(data._1 ++ data._2)}
    //new GenericRow(data._1 ++ data._2)
    // Creating schema for the new frame
    val newSchema = predictFrameRDD.frameSchema.addColumn("predicted_cluster", DataTypes.float64)
    val outputFrameRDD = new FrameRDD(newSchema, result)

      tryNew(Some(Naming.generateName(Some("predicted_frame")))) { newPredictedFrame: FrameMeta =>
        save(new SparkFrameData(
          newPredictedFrame.meta, outputFrameRDD))
      }.meta
  }

  }
