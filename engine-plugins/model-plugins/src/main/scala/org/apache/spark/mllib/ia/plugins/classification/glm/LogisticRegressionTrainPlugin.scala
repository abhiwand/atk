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

package org.apache.spark.mllib.ia.plugins.classification.glm

import com.intel.taproot.analytics.domain.CreateEntityArgs
import com.intel.taproot.analytics.domain.frame.FrameMeta
import com.intel.taproot.analytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.taproot.analytics.engine.spark.frame.SparkFrameData
import com.intel.taproot.analytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.taproot.analytics.engine.plugin.PluginDoc

//Implicits needed for JSON conversion
import spray.json._

import com.intel.taproot.analytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

/**
 * Parameters
 * ----------
 * frame : Frame
 *   A frame to train the model on.
 * label_column : str
 *   Column containing the label
 * observation_columns : list of str
 *   Columns containing the observations.
 * frequency_column : str (optional)
 *   Column containing frequency of the observation.
 * optimizer : str (optional)
 *   String containing the name of the optmizer for logistic regression
 *   Default is "LBFGS"
 * intercept : boolean (optional)
 *   Set if the algorithm should add an intercept.
 *   Default true.
 * feature_scaling : boolean (optional)
 *   Set if the algorithm should use feature scaling to improve the convergence during optimization.
 *   Default false.
 * num_iterations : int (optional)
 *   The maximal number of iterations.
 *   Default 100.
 * step_size : int (optional)
 *   The initial step size of SGD for the first step.
 *   In subsequent steps, the step size will decrease with stepSize/sqrt(t)
 *   Default 1.0.
 * reg_type : str (optional)
 *   Updater function to actually perform a gradient step in a given direction.
 *   The updater is responsible to perform the update from the regularization term as well,
 *   and therefore determines what kind or regularization is used, if any.
 * reg_param : double (optional)
 *   Regularization parameter.
 *   Default 0.0.
 * mini_batch_fraction : double (optional)
 *   Fraction of data to be used for each SGD iteration.
 *   Default 1.0 (corresponding to deterministic/classical gradient descent)
 * threshold : double (optional)
 *   Threshold for classification.
 *   Default is 0.5
 * num_classes : int (optional)
 *   Number of classes  .
 *   Default is 2.
 *   SGD is a binary classifier, LBFGS can be multiclass
 * convergence_tolerance : double (optional)
 *   Set the convergence tolerance of iterations for L-BFGS.
 *   Default 1E-4.
 * num_corrections : int (optional)
 *   Set the number of corrections used in the LBFGS update.
 *   Default 10.
 */

@PluginDoc(oneLine = "Build logistic regression model.",
  extended = "Creating a LogisticRegression Model using the observation column and label column of the train frame.",
  returns = """
    Results.
    The data returned is composed of multiple components:
numFeatures : Int
    Number of features in the training data
numClasses : Int
    Number of classes in the training data
coefficients: dict
    Value for each of the coefficients trained
covarianceMatrix: Frame (optional)
    Covariance matrix of the trained model.""""")
class LogisticRegressionTrainPlugin extends SparkCommandPlugin[LogisticRegressionTrainArgs, LogisticRegressionSummaryTable] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/train"

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
  override def execute(arguments: LogisticRegressionTrainArgs)(implicit invocation: Invocation): LogisticRegressionSummaryTable =
    {
      val models = engine.models
      val frames = engine.frames

      val inputFrame = frames.expectFrame(arguments.frame)
      val modelMeta = models.expectModel(arguments.model)

      //create RDD from the frame
      val trainFrameRdd = frames.loadFrameData(sc, inputFrame)
      val labeledTrainRdd = trainFrameRdd.toLabeledPointRDDWithFrequency(arguments.labelColumn,
        arguments.observationColumns, arguments.frequencyColumn)

      //Running MLLib
      val model = IaLogisticRegressionModelFactory.createModel(arguments)
      val logRegModel = model.getModel.run(labeledTrainRdd)

      //Compute optional covariance matrix
      val frameColumns = arguments.observationColumns :+ "intercept"
      model.getModel
      val covarianceFrame = ApproximateCovarianceMatrix(model)
        .toFrameRdd(labeledTrainRdd.sparkContext, frameColumns) match {
          case Some(frameRdd) => {
            val frame = tryNew(CreateEntityArgs(
              description = Some("covariance matrix created by LogisticRegression train operation"))) {
              newTrainFrame: FrameMeta =>
                save(new SparkFrameData(newTrainFrame.meta, frameRdd))
            }.meta
            Some(frame)
          }
          case _ => None
        }

      // Save model to metastore and return results
      val jsonModel = new LogisticRegressionData(logRegModel, arguments.observationColumns)
      //TODO: Call save instead once implemented for models
      models.updateModel(modelMeta.toReference, jsonModel.toJson.asJsObject)
      new LogisticRegressionSummaryTable(logRegModel, arguments.observationColumns, covarianceFrame)
    }
}
