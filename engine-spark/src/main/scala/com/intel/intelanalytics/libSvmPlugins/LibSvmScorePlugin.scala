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

import java.util.StringTokenizer

import com.intel.intelanalytics.domain.DoubleValue
import com.intel.intelanalytics.engine.plugin.{ CommandPlugin, ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import libsvm.{ svm_model, svm, svm_node }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.libsvm.ia.plugins.LibSvmJsonProtocol._

class LibSvmScorePlugin extends CommandPlugin[LibSvmScoreArgs, DoubleValue] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/score"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: LibSvmScoreArgs)(implicit invocation: Invocation) = 1

  /**
   * Get the predictions for observations in a vector
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */

  override def execute(arguments: LibSvmScoreArgs)(implicit invocation: Invocation): DoubleValue = {
    val models = engine.models
    val modelMeta = models.expectModel(arguments.model)

    val svmJsObject = modelMeta.data.get
    val libsvmData = svmJsObject.convertTo[LibSvmData]
    val libsvmModel = libsvmData.svmModel

    LibSvmPluginFunctions.score(libsvmModel, arguments.vector)
  }
}

object LibSvmPluginFunctions extends Serializable {

  def score(libsvmModel: svm_model, vector: Vector[Double]): DoubleValue = {
    val output = columnFormatter(vector.toArray.zipWithIndex)

    val splitObs: StringTokenizer = new StringTokenizer(output, " \t\n\r\f:")
    splitObs.nextToken()
    val counter: Int = splitObs.countTokens / 2
    val x: Array[svm_node] = new Array[svm_node](counter)
    var j: Int = 0
    while (j < counter) {
      x(j) = new svm_node
      x(j).index = atoi(splitObs.nextToken) + 1
      x(j).value = atof(splitObs.nextToken)
      j += 1
    }
    new DoubleValue(svm.svm_predict(libsvmModel, x))
  }

  def LibSvmFormatter(frameRdd: FrameRdd): Array[String] = {
    frameRdd.map(row => columnFormatterForTrain(row.toArray.zipWithIndex)).collect()
  }

  private def columnFormatterForTrain(valueIndexPairArray: Array[(Any, Int)]): String = {
    val result = for {
      i <- valueIndexPairArray
      value = i._1
      index = i._2
      if index != 0 && value != 0
    } yield s"$index:$value"
    s"${valueIndexPairArray(0)._1} ${result.mkString(" ")}"
  }

  private def columnFormatter(valueIndexPairArray: Array[(Any, Int)]): String = {
    val result = for {
      i <- valueIndexPairArray
      value = i._1
      index = i._2
      if value != 0
    } yield s"$index:$value"
    s"${valueIndexPairArray(0)._1} ${result.mkString(" ")}"
  }

  def atof(s: String): Double = {
    s.toDouble
  }

  def atoi(s: String): Int = {
    Integer.parseInt(s)
  }
}