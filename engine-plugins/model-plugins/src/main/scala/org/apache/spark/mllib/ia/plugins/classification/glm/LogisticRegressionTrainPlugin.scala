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

import com.intel.intelanalytics.domain.CreateEntityArgs
import com.intel.intelanalytics.domain.frame.FrameMeta
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, Invocation }
import com.intel.intelanalytics.engine.spark.frame.SparkFrameData
import com.intel.intelanalytics.engine.spark.plugin.SparkCommandPlugin
import com.intel.intelanalytics.engine.plugin.PluginDoc

//Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._
import org.apache.spark.mllib.ia.plugins.MLLibJsonProtocol._

@PluginDoc(oneLine = "Build logistic regression model.",
  extended = """Creating a LogisticRegression Model using the observation column and label
column of the train frame.""")
class LogisticRegressionTrainPlugin extends SparkCommandPlugin[LogisticRegressionTrainArgs, LogisticRegressionTrainResults] {
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
  override def execute(arguments: LogisticRegressionTrainArgs)(implicit invocation: Invocation): LogisticRegressionTrainResults =
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
      val covarianceFrame = ApproximateCovarianceMatrix(model)
        .toFrameRdd(labeledTrainRdd.sparkContext, "covariance") match {
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
      new LogisticRegressionTrainResults(logRegModel, arguments.observationColumns, covarianceFrame)
    }
}
