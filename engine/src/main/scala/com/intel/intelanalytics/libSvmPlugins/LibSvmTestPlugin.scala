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

package com.intel.intelanalytics.libSvmPlugins

import com.intel.intelanalytics.domain.frame.ClassificationMetricValue
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.frame.plugins.classificationmetrics.ClassificationMetrics
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.libsvm.ia.plugins.LibSvmJsonProtocol._
import org.apache.spark.rdd.RDD

class LibSvmTestPlugin extends SparkCommandPlugin[LibSvmTestArgs, ClassificationMetricValue] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/test"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: LibSvmTestArgs)(implicit invocation: Invocation) = 2

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LibSvmTestArgs)(implicit invocation: Invocation): ClassificationMetricValue = {
    val models = engine.models
    val modelMeta = models.expectModel(arguments.model)

    val frames = engine.frames
    val inputFrame = frames.expectFrame(arguments.frame)

    val frame: SparkFrameData = resolve(arguments.frame)

    // load frame as RDD
    val inputFrameRdd = frame.data

    //Loading the model
    val svmColumns = arguments.observationColumns
    val svmJsObject = modelMeta.data.get
    val libsvmData = svmJsObject.convertTo[LibSvmData]
    val libsvmModel = libsvmData.svmModel

    if (arguments.observationColumns.isDefined) {
      require(libsvmData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and test should be same")
    }

    //predicting a label for the observation column/s
    val predictionsRdd: RDD[Row] = inputFrameRdd.mapRows(row => {
      val array = row.valuesAsArray(arguments.observationColumns.getOrElse(libsvmData.observationColumns))
      val label = row.value(arguments.labelColumn)
      val doubles = array.map(i => DataTypes.toDouble(i))
      var vector = Vector.empty[Double]
      var i: Int = 0
      while (i < doubles.length) {
        vector = vector :+ doubles(i)
        i += 1
      }
      val predictionLabel = LibSvmPluginFunctions.score(libsvmModel, vector)
      Array[Any](label.asInstanceOf[Double], predictionLabel.value)
    })

    //Run Binary classification metrics
    val posLabel: String = "1.0"
    ClassificationMetrics.binaryClassificationMetrics(predictionsRdd, 0, 1, posLabel, 1)
  }

}