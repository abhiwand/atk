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

package org.apache.spark.mllib.ia.plugins.classification

import com.intel.intelanalytics.UnitReturn
import com.intel.intelanalytics.domain.command.CommandDoc
import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.model.ModelReference
import org.apache.spark.mllib.ia.plugins.classification.ClassificationWithSGDTestArgs
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionWithSGD }
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }
import org.apache.spark.mllib.regression.{ GeneralizedLinearAlgorithm, LabeledPoint }
import org.apache.spark.rdd.RDD
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

//Implicits needed for JSON conversion
import spray.json._

case class LogisticRegressionTrainArgs(model: ModelReference,
                                       frame: FrameReference,
                                       optimizer: String,
                                       labelColumn: String,
                                       observationColumns: List[String],
                                       intercept: Option[Boolean] = None,
                                       featureScaling: Option[Boolean] = None,
                                       numIterations: Option[Int] = None,
                                       stepSize: Option[Int] = None,
                                       regType: Option[String] = None,
                                       regParam: Option[Double] = None,
                                       miniBatchFraction: Option[Double] = None,
                                       threshold: Option[Double] = None,
                                       //TODO: What input type should this be?
                                       //gradient : Option[Double] = None,
                                       numClasses: Option[Int] = None,
                                       convergenceTolerance: Option[Double] = None,
                                       numCorrections: Option[Int] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(optimizer == "LBFGS" || optimizer == "SGD", "valid optimizer name needed")
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")

  def getNumIterations: Int = {
    if (numIterations.isDefined) { require(numIterations.get > 0, "numIterations must be a positive value") }
    numIterations.getOrElse(100)
  }

  def getIntercept: Boolean = { intercept.getOrElse(true) }
  def getStepSize: Int = { stepSize.getOrElse(1) }
  def getRegParam: Double = { regParam.getOrElse(0.01) }
  def getMiniBatchFraction: Double = { miniBatchFraction.getOrElse(1.0) }
  def getFeatureScaling: Boolean = { featureScaling.getOrElse(false) }
  def getNumClasses: Int = { numClasses.getOrElse(2) }
  //TODO: Verify what this should be default
  def getConvergenceTolerance: Double = { convergenceTolerance.getOrElse(0.01) }
  //TODO: Verify what this should be default
  def getNumCorrections: Int = { numCorrections.getOrElse(2) }
  def getThreshold: Double = { threshold.getOrElse(0.5) }
}

case class LogisticRegressionReturnArgs(numFeatures: Int, numClasses: Int)

@PluginDoc(oneLine = "Build logistic regression model.",
  extended = """Creating a LogisticRegression Model using the observation column and label
column of the train frame.""")
class LogisticRegressionTrainPlugin extends SparkCommandPlugin[LogisticRegressionTrainArgs, LogisticRegressionReturnArgs] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression_exp/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: LogisticRegressionTrainArgs)(implicit invocation: Invocation) = 109
  /**
   * Run MLLib's LogisticRegressionWithSGD() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LogisticRegressionTrainArgs)(implicit invocation: Invocation): LogisticRegressionReturnArgs =
    {
      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame)
      val modelMeta = models.expectModel(arguments.model)

      //create RDD from the frame
      val trainFrameRdd = frames.loadFrameData(sc, inputFrame)
      val labeledTrainRdd: RDD[LabeledPoint] = trainFrameRdd.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)

      //Running MLLib
      val model = arguments.optimizer match {
        case "LBFGS" => initializeLBFGSModel(arguments)
        case "SGD" => initializeSGDModel(arguments)
        case _ => throw new IllegalArgumentException("Only LBFGS or SGD optimizers permitted")
      }

      val logRegModel = model.run(labeledTrainRdd)
      val jsonModel = new LogisticRegressionWithSGDData(logRegModel, arguments.observationColumns)

      //TODO: Call save instead once implemented for models
      models.updateModel(modelMeta.toReference, jsonModel.toJson.asJsObject)
      LogisticRegressionReturnArgs(logRegModel.numFeatures, logRegModel.numClasses)
      //new UnitReturn
    }

  private def initializeLBFGSModel(arguments: LogisticRegressionTrainArgs): LogisticRegressionWithLBFGS = {
    val model = new LogisticRegressionWithLBFGS()

    model.optimizer.setNumIterations(arguments.getNumIterations)
    model.optimizer.setConvergenceTol(arguments.getConvergenceTolerance)
    model.optimizer.setNumCorrections(arguments.getNumCorrections)
    model.optimizer.setRegParam(arguments.getRegParam)

    model.setNumClasses(arguments.getNumClasses)
    model.setFeatureScaling(arguments.getFeatureScaling)

    if (arguments.regType.isDefined) {
      model.optimizer.setUpdater(arguments.regType.get match {
        case "L1" => new L1Updater()
        case other => new SquaredL2Updater()
      })
    }
    model.setIntercept(arguments.getIntercept)
  }

  private def initializeSGDModel(arguments: LogisticRegressionTrainArgs): LogisticRegressionWithSGD = {
    val model = new LogisticRegressionWithSGD()

    model.optimizer.setNumIterations(arguments.getNumIterations)
    model.optimizer.setStepSize(arguments.getStepSize)
    model.optimizer.setRegParam(arguments.getRegParam)
    model.optimizer.setMiniBatchFraction(arguments.getMiniBatchFraction)

    model.setFeatureScaling(arguments.getFeatureScaling)

    if (arguments.regType.isDefined) {
      model.optimizer.setUpdater(arguments.regType.get match {
        case "L1" => new L1Updater()
        case other => new SquaredL2Updater()
      })
    }
    model.setIntercept(arguments.getIntercept)

  }

}
