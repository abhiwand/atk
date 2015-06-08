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
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }

//Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Parameters
 * ----------
 * frame : Frame
 * A frame to train the model on.
 * label_column : str
 * Column name containing the label for each observation.
 * observation_column : list of str
 * Column(s) containing the observations.
 * intercept : bool (Optional)
 * The algorithm adds an intercept. Default is true.
 * num_iterations: int (Optional)
 * Number of iterations. Default is 100.
 * step_size: int (Optional)
 * Step size for optimizer. Default is 1.0.
 * reg_type: str (Optional)
 * Regularization L1 or L2. Default is L2.
 * reg_param: double (Optional)
 * Regularization parameter. Default is 0.01.
 * mini_batch_fraction : double (Optional)
 * Mini batch fraction parameter. Default is 1.0.
 */

@PluginDoc(oneLine = "Train SVM model based on another frame.",
  extended = """Creating a SVM Model using the observation column and label column of the train frame.""")
class SVMWithSGDTrainPlugin extends SparkCommandPlugin[ClassificationWithSGDTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:svm/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ClassificationWithSGDTrainArgs)(implicit invocation: Invocation) = 103
  /**
   * Run MLLib's SVMWithSGD() on the training frame and create a Model for it.
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
      val modelRef = arguments.model
      val modelMeta = models.expectModel(modelRef)

      val frame: SparkFrameData = resolve(arguments.frame)
      // load frame as RDD
      val trainFrameRdd = frame.data

      val labeledTrainRDD: RDD[LabeledPoint] = trainFrameRdd.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)

      //Running MLLib
      val svm = initializeSVMModel(arguments)
      val svmModel = svm.run(labeledTrainRDD)

      val jsonModel = new SVMData(svmModel, arguments.observationColumns)

      //TODO: Call save instead once implemented for models
      models.updateModel(modelMeta.toReference, jsonModel.toJson.asJsObject)
      new UnitReturn
    }

  private def initializeSVMModel(arguments: ClassificationWithSGDTrainArgs): SVMWithSGD = {
    val svm = new SVMWithSGD()
    svm.optimizer.setNumIterations(arguments.getNumIterations)
    svm.optimizer.setStepSize(arguments.getStepSize)
    svm.optimizer.setRegParam(arguments.getRegParam)

    if (arguments.regType.isDefined) {
      svm.optimizer.setUpdater(arguments.regType.get match {
        case "L1" => new L1Updater()
        case other => new SquaredL2Updater()
      })
    }
    svm.optimizer.setMiniBatchFraction(arguments.getMiniBatchFraction)
    svm.setIntercept(arguments.getIntercept)
  }
}
