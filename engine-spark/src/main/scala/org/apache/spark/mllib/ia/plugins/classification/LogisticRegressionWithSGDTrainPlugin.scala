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

package org.apache.spark.mllib.ia.plugins.classification

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import org.apache.spark.mllib.ia.plugins.classification.ClassificationWithSGDTestArgs
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

//Implicits needed for JSON conversion
import spray.json._

class LogisticRegressionWithSGDTrainPlugin extends SparkCommandPlugin[ClassificationWithSGDTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/train"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ClassificationWithSGDTrainArgs)(implicit invocation: Invocation) = 109
  /**
   * Run MLLib's LogisticRegressionWithSGD() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationWithSGDTrainArgs)(implicit invocation: Invocation): UnitReturn =
    {
      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame.id)
      val modelMeta = models.expectModel(arguments.model.id)

      //create RDD from the frame
      val trainFrameRDD = frames.loadFrameData(sc, inputFrame)
      val labeledTrainRDD: RDD[LabeledPoint] = trainFrameRDD.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)

      //Running MLLib
      val logReg = initializeLogisticRegressionModel(arguments)

      val logRegModel = logReg.run(labeledTrainRDD)
      val jsonModel = new LogisticRegressionData(logRegModel, arguments.observationColumns)

      //TODO: Call save instead once implemented for models
      models.updateModel(modelMeta, jsonModel.toJson.asJsObject)
      new UnitReturn
    }
  private def initializeLogisticRegressionModel(arguments: ClassificationWithSGDTrainArgs): LogisticRegressionWithSGD = {
    val logReg = new LogisticRegressionWithSGD()
    logReg.optimizer.setNumIterations(arguments.getNumIterations)
    logReg.optimizer.setStepSize(arguments.getStepSize)
    logReg.optimizer.setRegParam(arguments.getRegParam)

    if (arguments.regType.isDefined) {
      logReg.optimizer.setUpdater(arguments.regType.get match {
        case "L1" => new L1Updater()
        case other => new SquaredL2Updater()
      })
    }
    logReg.optimizer.setMiniBatchFraction(arguments.getMiniBatchFraction)
    logReg.setIntercept(arguments.getIntercept)
  }
}
