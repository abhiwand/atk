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

package com.intel.intelanalytics.libSvmPlugins

import com.intel.intelanalytics.domain.{ CreateEntityArgs }
import com.intel.intelanalytics.domain.frame.{ FrameEntity, FrameMeta }
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import org.apache.spark.frame.FrameRdd
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.libsvm.ia.plugins.LibSvmJsonProtocol._

// TODO: all plugins should move out of engine-core into plugin modules

class LibSvmPredictPlugin extends SparkCommandPlugin[LibSvmPredictArgs, FrameEntity] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: LibSvmPredictArgs)(implicit invocation: Invocation) = 2

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LibSvmPredictArgs)(implicit invocation: Invocation): FrameEntity = {
    val models = engine.models
    val modelMeta = models.expectModel(arguments.model)

    val frame: SparkFrameData = resolve(arguments.frame)

    // load frame as RDD
    val inputFrameRdd = frame.data

    //Load the libsvm model
    val svmColumns = arguments.observationColumns
    val svmJsObject = modelMeta.data.get
    val libsvmData = svmJsObject.convertTo[LibSvmData]
    val libsvmModel = libsvmData.svmModel

    if (arguments.observationColumns.isDefined) {
      require(libsvmData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    //predicting a label for the observation column/s
    val predictionsRdd = inputFrameRdd.mapRows(row => {
      val array = row.valuesAsArray(arguments.observationColumns.getOrElse(libsvmData.observationColumns))
      val doubles = array.map(i => DataTypes.toDouble(i))
      var vector = Vector.empty[Double]
      var i: Int = 0
      while (i < doubles.length) {
        vector = vector :+ doubles(i)
        i += 1
      }
      val predictionLabel = LibSvmPluginFunctions.score(libsvmModel, vector)
      row.addValue(predictionLabel.value)
    })

    val updatedSchema = inputFrameRdd.frameSchema.addColumn("predicted_label", DataTypes.float64)
    val predictFrameRdd = new FrameRdd(updatedSchema, predictionsRdd)

    tryNew(CreateEntityArgs(description = Some("created by LibSvm's predict operation"))) {
      newPredictedFrame: FrameMeta =>
        save(new SparkFrameData(newPredictedFrame.meta, predictFrameRdd))
    }.meta
  }
}
