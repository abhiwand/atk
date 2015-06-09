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

package org.apache.spark.mllib.ia.plugins.clustering

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.{ CreateEntityArgs, Naming }
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame._
import com.intel.intelanalytics.domain.schema.Column
import com.intel.intelanalytics.domain.schema.{ FrameSchema, DataTypes }
import com.intel.intelanalytics.domain.schema.DataTypes._
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
import com.intel.intelanalytics.engine.spark.frame.{ SparkFrameData }
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._
import org.apache.spark.mllib.ia.plugins.VectorUtils._

import scala.collection.mutable.ListBuffer

/**
 * Parameters
 * ----------
 * predict_frame : Frame
 *   A frame whose labels are to be predicted.
 *   By default, predict is run on the same columns over which the model is
 *   trained.
 *   The user could specify column names too if needed.
 * observation_columns : list of str (optional)
 *   Column(s) containing the observations whose clusters are to be predicted.
 *   By default, we predict the clusters over columns the KMeansModel was
 *   trained on.
 *   The columns are scaled using the same values used when training the model.
 */

@PluginDoc(oneLine = "Predict the cluster assignments for the data points.",
  extended = "",
  returns = """Frame
    A new frame consisting of the existing columns of the frame and new columns.
    The data returned is composed of multiple components:
'k' columns : double
    Containing squared distance of each point to every cluster center.
predicted_cluster : int
    Integer containing the cluster assignment.""")
class KMeansPredictPlugin extends SparkCommandPlugin[KMeansPredictArgs, FrameEntity] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:k_means/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

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
    val inputFrameRdd = frames.loadFrameData(sc, inputFrame)

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
    val predictionsRDD = inputFrameRdd.mapRows(row => {
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
    val updatedSchema = inputFrameRdd.frameSchema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })
    val predictFrameRdd = new FrameRdd(updatedSchema, predictionsRDD)

    tryNew(CreateEntityArgs(description = Some("created by KMeans predict operation"))) { newPredictedFrame: FrameMeta =>
      save(new SparkFrameData(
        newPredictedFrame.meta, predictFrameRdd))
    }.meta
  }

}
