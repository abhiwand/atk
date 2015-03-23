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

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.frame.FrameRDD
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._
import com.intel.libSvm.libsvm.java.libsvm._

//Implicits needed for JSON conversion
import spray.json._

class LibSvmTrainPlugin extends SparkCommandPlugin[LibSvmTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: LibSvmTrainArgs)(implicit invocation: Invocation) = 1

  /**
   * Run LibSvm on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */

  override def execute(arguments: LibSvmTrainArgs)(implicit invocation: Invocation): UnitReturn = {
    val models = engine.models
    val modelRef = arguments.model
    val modelMeta = models.expectModel(modelRef)

    val frame: SparkFrameData = resolve(arguments.frame)
    // load frame as RDD
    val trainFrameRDD = frame.data

    //Running LibSVM
    val param = initializeParameters(arguments)
    val prob = initializeProblem(trainFrameRDD, arguments, param)

    val mySvmModel = svm.svm_train(prob, param)

    val jsonModel = new LibSvmData(mySvmModel, arguments.observationColumns)

    //TODO: Call save instead once implemented for models
    models.updateModel(modelMeta.toReference, jsonModel.toJson.asJsObject)
    new UnitReturn

  }

  private def initializeParameters(arguments: LibSvmTrainArgs): svm_parameter = {
    val param = new svm_parameter()

    // values for svm_parameters
    param.svm_type = svm_parameter.C_SVC
    param.kernel_type = svm_parameter.RBF
    param.degree = arguments.getDegree
    param.gamma = arguments.getGamma
    param.coef0 = arguments.getCoef0
    param.nu = arguments.getNu
    param.cache_size = arguments.getCacheSize
    param.C = arguments.getC
    param.eps = arguments.getEpsilon
    param.p = arguments.getP
    param.shrinking = arguments.getShrinking
    param.probability = arguments.getProbability
    param.nr_weight = arguments.getNrWeight
    param.weight_label = new Array[Int](0)
    param.weight = new Array[Double](0)
    param
  }

  private def initializeProblem(trainFrameRdd: FrameRDD, arguments: LibSvmTrainArgs, param: svm_parameter): svm_problem = {
    trainFrameRdd.frameSchema.requireColumnIsType(arguments.labelColumn, DataTypes.float64)

    val observedRdd = trainFrameRdd.selectColumns(arguments.labelColumn +: arguments.observationColumns)
    val observedSchema = observedRdd.frameSchema.columns
    def columnFormatter(valueIndexPairArray: Array[(Any, Int)]): String = {
      val result = for {
        i <- valueIndexPairArray
        value = i._1
        index = i._2
        if index != 0 && value != 0
      } yield s"$index:$value"
      s"${valueIndexPairArray(0)._1} ${result.mkString(" ")}"
    }
    val output = observedRdd.map(row => columnFormatter(row.toArray.zipWithIndex)).collect()

    val vectory: Vector[Double] = null
    val vectorx: Vector[Array[svm_node]] = null
    var max_index: Int = 0
    val prob = new svm_problem()

    for (i <- 0 until output.length) {
      val observation = output(i)
      val splitObs: StringTokenizer = new StringTokenizer(observation, " \t\n\r\f:")

      vectory :+ atof(splitObs.nextToken)
      val counter: Int = splitObs.countTokens / 2
      val x: Array[svm_node] = new Array[svm_node](counter)

      var j: Int = 0
      while (j < counter) {
        x(j) = new svm_node
        x(j).index = atoi(splitObs.nextToken)
        x(j).value = atof(splitObs.nextToken)
        j += 1
      }
      if (counter > 0) max_index = Math.max(max_index, x(counter - 1).index)
      vectorx :+ x
    }

    prob.l = vectory.size
    val myprobx = Array.ofDim[Array[svm_node]](prob.l)
    var k: Int = 0
    while (k < prob.l) {
      myprobx(k) = vectorx(k)
      k += 1
    }
    prob.x = myprobx

    val myproby = Array.ofDim[Double](prob.l)
    var i: Int = 0
    while (i < prob.l) {
      myproby(i) = vectory(i)
      i += 1
    }
    prob.y = myproby

    if (param.gamma == 0 && max_index > 0) param.gamma = 1.0 / max_index
    prob
  }

  private def atof(s: String): Double = {
    s.toDouble
  }

  private def atoi(s: String): Int = {
    Integer.parseInt(s)
  }

}