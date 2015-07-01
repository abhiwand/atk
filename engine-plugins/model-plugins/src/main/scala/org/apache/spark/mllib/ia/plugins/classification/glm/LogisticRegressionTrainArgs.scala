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

import com.intel.intelanalytics.domain.frame.FrameReference
import com.intel.intelanalytics.domain.model.ModelReference

/**
 * Input arguments for logistic regression train plugin
 */
case class LogisticRegressionTrainArgs(model: ModelReference,
                                       frame: FrameReference,
                                       optimizer: String,
                                       labelColumn: String,
                                       observationColumns: List[String],
                                       frequencyColumn: Option[String] = None,
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
                                       numCorrections: Option[Int] = None,
                                       computeHessian: Option[Boolean] = None) {
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
  def getComputeHessian : Boolean = {computeHessian.getOrElse(false)}
}

