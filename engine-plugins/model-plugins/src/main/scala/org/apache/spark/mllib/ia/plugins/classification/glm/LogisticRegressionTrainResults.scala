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

import com.intel.taproot.analytics.domain.frame.FrameEntity
import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency

/**
 * Results for logistic regression train plugin
 *
 * @param numFeatures Number of features
 * @param numClasses Number of classes
 * @param coefficients Model coefficients
 * @param covarianceMatrix Optional covariance matrix
 */
case class LogisticRegressionTrainResults(numFeatures: Int,
                                          numClasses: Int,
                                          coefficients: Map[String, Double],
                                          covarianceMatrix: Option[FrameEntity]) {

  def this(logRegModel: LogisticRegressionModelWithFrequency,
           observationColumns: List[String],
           covarianceMatrix: Option[FrameEntity]) = {
    this(logRegModel.numFeatures,
      logRegModel.numClasses,
      LogisticRegressionTrainResults.getCoefficients(logRegModel, observationColumns),
      covarianceMatrix)
  }

}

object LogisticRegressionTrainResults {
  /**
   * Get map of model coefficients
   * @param logRegModel Logistic regression model
   * @param observationColumns Observation columns
   * @return Map with names and values of model coefficients
   */
  def getCoefficients(logRegModel: LogisticRegressionModelWithFrequency,
                      observationColumns: List[String]): Map[String, Double] = {
    val coefficients = logRegModel.intercept +: logRegModel.weights.toArray
    val coefficientNames = List("intercept") ++ observationColumns
    (coefficientNames zip coefficients).toMap
  }
}