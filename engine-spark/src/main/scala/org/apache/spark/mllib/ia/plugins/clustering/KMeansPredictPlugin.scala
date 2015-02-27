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

package org.apache.spark.mllib.ia.plugins.clustering

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.{ CreateEntityArgs, Naming }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.domain.schema.{ FrameSchema, DataTypes }
import com.intel.intelanalytics.domain.schema.DataTypes._
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData }
import org.apache.spark.frame.FrameRDD
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._
import org.apache.spark.mllib.ia.plugins.VectorUtils._

import scala.collection.mutable.ListBuffer

class KMeansPredictPlugin extends SparkCommandPlugin[KMeansPredictArgs, FrameEntity] {

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
  override def execute(arguments: KMeansPredictArgs)(implicit invocation: Invocation): FrameEntity = {

    val models = engine.models
    val frames = engine.frames

    val inputFrame = frames.expectFrame(arguments.frame)
    val modelMeta = models.expectModel(arguments.model)

    //create RDD from the frame
    val inputFrameRDD = frames.loadFrameData(sc, inputFrame)

    //Extracting the KMeansModel from the stored JsObject
    val kmeansJsObject = modelMeta.data.get
    val kmeansData = kmeansJsObject.convertTo[KMeansData]
    val kmeansModel = kmeansData.kMeansModel
    if (arguments.observationColumns.isDefined) {
      require(kmeansData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val kmeansColumns = arguments.observationColumns.getOrElse(kmeansData.observationColumns)
    val scalingValues = kmeansData.columnScalings

    //Predicting the cluster for each row
    val predictionsRDD = inputFrameRDD.mapRows(row => {
      val columnsArray = row.valuesAsArray(kmeansColumns).map(row => DataTypes.toDouble(row))
      val columnScalingsArray = scalingValues.toArray
      val doubles = columnsArray.zip(columnScalingsArray).map { case (x, y) => x * y }
      val point = Vectors.dense(doubles)

      val clusterCenters = kmeansModel.clusterCenters

      for (i <- 0 to (clusterCenters.length - 1)) {
        val distance = toMahoutVector(point).getDistanceSquared(toMahoutVector(clusterCenters(i)))
        row.addValue(distance)
      }
      val prediction = kmeansModel.predict(point)
      row.addValue(prediction + 1)
    })

    //Updating the frame schema
    var columnNames = new ListBuffer[String]()
    var columnTypes = new ListBuffer[DataTypes.DataType]()
    for (i <- 1 to (kmeansModel.clusterCenters.length)) {
      val colName = "distance_from_cluster_" + i.toString
      columnNames += colName
      columnTypes += DataTypes.float64
    }
    columnNames += "predicted_cluster"
    columnTypes += DataTypes.int32

    val newColumns = columnNames.toList.zip(columnTypes.toList.map(x => x: DataType))
    val updatedSchema = inputFrameRDD.frameSchema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })
    val predictFrameRDD = new FrameRDD(updatedSchema, predictionsRDD)

    tryNew(CreateEntityArgs(description = Some("created by KMeans predict operation"))) { newPredictedFrame: FrameMeta =>
      save(new SparkFrameData(
        newPredictedFrame.meta, predictFrameRDD))
    }.meta
  }

}
